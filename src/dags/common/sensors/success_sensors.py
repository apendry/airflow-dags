import os
import pendulum

from datetime import datetime, timedelta
from functools import cached_property
from typing import Any, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.hooks.filesystem import FSHook
from airflow.models import Variable
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.triggers.base import StartTriggerArgs
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults

from .triggers.files import FileLatestTrigger, __check_interval_paths__
from ..env import global_config
from ..enums.frequency import Frequency
from ..datasets import Dataset
from ..sensors.triggers.files import FileIntervalTrigger

class BaseDatasetSensor(BaseSensorOperator):

    template_fields: Sequence[str] = ("target_date",)

    @apply_defaults
    def __init__(
            self,
            *,
            task_id,
            dataset: Dataset,
            target_date,
            intervals,
            fs_conn_id=global_config[Variable.get("env")]["fs_conn"],
            poke_interval=None,
            timeout=None,
            deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
            start_from_trigger: bool = False,
            trigger_kwargs: dict[str, Any] | None = None,
            **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            poke_interval=int(pendulum.duration(minutes=5).total_seconds()) if not poke_interval else poke_interval,
            timeout=dataset.timeout if not timeout else timeout,
            **kwargs
        )
        self.dataset = dataset
        self.target_date = target_date
        self.intervals = intervals
        self.fs_conn_id = fs_conn_id
        self.deferrable = deferrable
        self.start_from_trigger = start_from_trigger


    @cached_property
    def paths(self) -> list:
        hook = FSHook(self.fs_conn_id)
        base_path = hook.get_path() + "/data"
        dt_start = pendulum.parse(self.target_date)
        dt_end = dt_start.add(
            months=self.intervals if self.dataset.frequency == Frequency.MONTHLY else 0,
            days=self.intervals if self.dataset.frequency == Frequency.DAILY else 0,
            hours=self.intervals if self.dataset.frequency == Frequency.HOURLY else 0
        )
        date_times = pendulum.interval(dt_start, dt_end).range(self.dataset.frequency.value)
        return [os.path.join(base_path, dt.strftime(self.dataset.path_template)) + "_SUCCESS" for dt in date_times]


class DatasetIntervalSensor(BaseDatasetSensor):

    template_fields: Sequence[str] = ("target_date",)
    start_trigger_args = StartTriggerArgs(
        trigger_cls="src.common.sensors.triggers.FileIntervalTrigger",
        trigger_kwargs={},
        next_method="execute_complete",
        next_kwargs=None,
        timeout=None,
    )

    @apply_defaults
    def __init__(
        self,
        *,
        dataset: Dataset,
        target_date,
        intervals,
        task_id=None,
        poke_interval=None,
        timeout=None,
        **kwargs,
    ):
        super().__init__(
            dataset=dataset,
            target_date=target_date,
            intervals=intervals,
            task_id=f"{dataset.name.lower()}_input_sensor" if not task_id else task_id,
            poke_interval=poke_interval,
            timeout=timeout,
            **kwargs
        )

        self.xcom_key = f"input_{self.dataset.name.lower()}"

        if self.deferrable and self.start_from_trigger:
            self.start_trigger_args.timeout = timedelta(seconds=self.timeout)
            self.start_trigger_args.trigger_kwargs = dict(
                dataset=self.dataset,
                paths=self.paths,
                recursive=False,
                poke_interval=self.poke_interval,
            )

    def poke(self, context: Context) -> PokeReturnValue:
        results = []
        for path in self.paths:
            __check_interval_paths__(path, self.log, results)

        return PokeReturnValue(is_done=all(results), xcom_value=self.paths)

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            self.xcom_push(context, self.xcom_key, super().execute(context=context))
        if not self.poke(context=context):
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=FileIntervalTrigger(
                    paths=self.paths,
                    poke_interval=self.poke_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: bool | None = None) -> None:
        if not event:
            raise AirflowException("%s task failed as no success files found in %s.", self.task_id, self.paths)
        self.log.info("%s completed successfully as all success files found.", self.task_id)
        self.xcom_push(context, self.xcom_key, self.paths)


class DatasetLatestSensor(BaseDatasetSensor):

    start_trigger_args = StartTriggerArgs(
        trigger_cls="src.common.sensors.triggers.FileLatestTrigger",
        trigger_kwargs={},
        next_method="execute_complete",
        next_kwargs=None,
        timeout=None,
    )

    @apply_defaults
    def __init__(
            self,
            *,
            dataset: Dataset,
            target_date,
            intervals,
            task_id=None,
            poke_interval=None,
            timeout=None,
            **kwargs
    ):

        super().__init__(
            task_id=f"{dataset.name.lower()}_input_sensor" if not task_id else task_id,
            poke_interval=int(pendulum.duration(minutes=5).total_seconds()) if not poke_interval else poke_interval,
            timeout=dataset.timeout if not timeout else timeout,
            dataset=dataset,
            target_date=target_date,
            intervals=intervals,
            **kwargs
        )

        self.xcom_key = f"input_{self.dataset.name.lower()}"

        if self.deferrable and self.start_from_trigger:
            self.start_trigger_args.timeout = timedelta(seconds=self.timeout)
            self.start_trigger_args.trigger_kwargs = dict(
                dataset=self.dataset,
                paths=self.paths,
                recursive=False,
                poke_interval=self.poke_interval,
            )

    def poke(self, context: Context) -> PokeReturnValue:
        self.paths.sort(reverse=True)
        for path in self.paths:
            self.log.info("Poking for file %s", path)
            if os.path.isfile(path):
                mod_time = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%dT%H:%M:%S")
                self.log.info("Found Latest File %s last modified: %s", path, mod_time)
                return PokeReturnValue(is_done=True, xcom_value=path)

            for _, _, files in os.walk(path):
                if files:
                    return PokeReturnValue(is_done=True, xcom_value=path)

        return PokeReturnValue(is_done=False)

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            self.xcom_push(context, self.xcom_key, super().execute(context=context))
        if not self.poke(context=context):
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=FileLatestTrigger(
                    paths=self.paths,
                    poke_interval=self.poke_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: str | None = None) -> None:
        if not event:
            raise AirflowException("%s task failed as no success files found in %s.", self.task_id, self.paths)
        self.log.info("%s completed successfully as latest success files found.", self.task_id)
        self.xcom_push(context, self.xcom_key, event)