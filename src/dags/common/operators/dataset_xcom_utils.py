import os
from typing import Sequence, Any

import pendulum
from airflow.hooks.filesystem import FSHook
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults

from ..enums.frequency import Frequency
from ..enums.xcom import TaskIds, Keys


class PushOutputDatasetPaths(BaseOperator):

    template_fields: Sequence[str] = ("target_date",)

    @apply_defaults
    def __init__(
            self,
            *,
            dataset,
            target_date,
            intervals = 0,
            fs_conn_id = "fs_default",
            **kwargs
    ):
        self.task_id = TaskIds.output(dataset)
        super().__init__(task_id = self.task_id, **kwargs)
        self.dataset = dataset
        self.target_date = target_date
        self.intervals = intervals
        self.fs_conn_id = fs_conn_id
        self.xcom_key = Keys.output(self.dataset)

    def execute(self, context: Context) -> None:
        hook = FSHook(self.fs_conn_id)
        base_path = hook.get_path()
        dt_start = pendulum.parse(self.target_date)
        if self.intervals == 0:
            xcom_value = os.path.join(base_path, dt_start.strftime(self.dataset.path_template))
        else:
            hook = FSHook(self.fs_conn_id)
            base_path = hook.get_path()
            dt_end = dt_start.add(
                months=self.intervals if self.dataset.frequency == Frequency.MONTHLY else 0,
                days=self.intervals if self.dataset.frequency == Frequency.DAILY else 0,
                hours=self.intervals if self.dataset.frequency == Frequency.HOURLY else 0
            )
            date_times = pendulum.interval(dt_start, dt_end).range(self.dataset.frequency.value)
            xcom_value = [os.path.join(base_path, dt.strftime(self.dataset.path_template)) for dt in date_times]

        self.xcom_push(context, self.xcom_key, xcom_value)

class PullDatasetPaths(BaseOperator):

    def __init__(
            self,
            dataset,
            task_id: str = None,
            **kwargs
    ):
        self.task_id = task_id if task_id else f"pull_{dataset.name.lower()}_paths"
        super().__init__(task_id=self.task_id, **kwargs)
        self.dataset = dataset

class PullInputPaths(PullDatasetPaths):

    def __init__(self, dataset, **kwargs):
        super().__init__(dataset=dataset, **kwargs)

    def execute(self, context: Context) -> Any:
        return self.xcom_pull(
            context=context,
            task_ids=TaskIds.input(self.dataset),
            key=Keys.input(self.dataset),
        )

class PullOutputPaths(PullDatasetPaths):
    def __init__(self, dataset, **kwargs):
        super().__init__(dataset=dataset, **kwargs)

    def execute(self, context: Context) -> Any:
        return self.xcom_pull(
            context=context,
            task_ids=TaskIds.output.value(self.dataset),
            key=Keys.output.value(self.dataset),
        )