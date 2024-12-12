import os
from typing import Sequence

import pendulum
from airflow.hooks.filesystem import FSHook
from airflow.models import BaseOperator
from airflow.utils.context import Context

from src.dags.common.enums.frequency import Frequency


class XcomPushDatasetPaths(BaseOperator):

    template_fields: Sequence[str] = ("target_date",)

    def __init__(
            self,
            dataset,
            target_date,
            intervals = 0,
            task_id = None,
            fs_conn_id = "fs_default",
            **kwargs
    ):
        super().__init__(**kwargs)
        self.dataset = dataset
        self.task_id = task_id if task_id else f"{dataset.name.lower()}_output_operator"
        self.dt_start = pendulum.parse(target_date)
        self.intervals = intervals
        self.fs_conn_id = fs_conn_id
        self.xcom_key = f"output_{self.dataset.name.lower()}"

    def execute(self, context: Context) -> None:
        hook = FSHook(self.fs_conn_id)
        base_path = hook.get_path()
        if self.intervals == 0:
            xcom_value = os.path.join(base_path, self.dt_start.strftime(self.dataset.path_template))
        else:
            hook = FSHook(self.fs_conn_id)
            base_path = hook.get_path()
            dt_end = self.dt_start.add(
                months=self.intervals if self.dataset.frequency == Frequency.MONTHLY else 0,
                days=self.intervals if self.dataset.frequency == Frequency.DAILY else 0,
                hours=self.intervals if self.dataset.frequency == Frequency.HOURLY else 0
            )
            date_times = pendulum.interval(self.dt_start, dt_end).range(self.dataset.frequency.value)
            xcom_value = [os.path.join(base_path, dt.strftime(self.dataset.path_template)) for dt in date_times]

        self.xcom_push(context, self.xcom_key, xcom_value)

# Convenience methods
def xcom_pull_input_paths(
        dataset,
        **kwargs
) -> str | list:
    return kwargs['ti'].xcom_pull(
        task_ids=f"{dataset.name.lower()}_input_sensor",
        key=f"input_{dataset.name.lower()}",
    )

def xcom_pull_output_path(
        dataset,
        **kwargs
) -> str:
    return kwargs['ti'].xcom_pull(
        task_ids=f"{dataset.name.lower()}_output_operator",
        key=f"output_{dataset.name.lower()}",
    )