import os
import pendulum

from datetime import datetime, timedelta
from functools import cached_property
from typing import Any, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.hooks.filesystem import FSHook
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import StartTriggerArgs
from airflow.utils.context import Context

from ..enums.frequency import Frequency
from ..datasets import Dataset
from ..sensors.triggers.files import FilesTrigger


class DatasetSensor(BaseSensorOperator):

    template_fields: Sequence[str] = ("target_date",)
    start_trigger_args = StartTriggerArgs(
        trigger_cls="src.common.sensors.triggers.FilesTrigger",
        trigger_kwargs={},
        next_method="execute_complete",
        next_kwargs=None,
        timeout=None,
    )
    start_from_trigger = False

    def __init__(
        self,
        *,
        dataset: Dataset,
        target_date,
        intervals,
        fs_conn_id="fs_default",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        start_from_trigger: bool = False,
        trigger_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dataset = dataset
        self.target_date = target_date
        self.intervals = intervals
        self.fs_conn_id = fs_conn_id
        self.deferrable = deferrable

        self.start_from_trigger = start_from_trigger

        if self.deferrable and self.start_from_trigger:
            self.start_trigger_args.timeout = timedelta(seconds=self.timeout)
            self.start_trigger_args.trigger_kwargs = dict(
                dataset=self.dataset,
                paths=self.paths,
                recursive=False,
                poke_interval=self.poke_interval,
            )

    @cached_property
    def paths(self) -> list:
        hook = FSHook(self.fs_conn_id)
        base_path = hook.get_path()
        dt_start = pendulum.parse(self.target_date)
        dt_end = dt_start.add(
            months=self.intervals if self.dataset.frequency == Frequency.MONTHLY else 0,
            days=self.intervals if self.dataset.frequency == Frequency.DAILY else 0,
            hours=self.intervals if self.dataset.frequency == Frequency.HOURLY else 0
        )
        date_times = pendulum.interval(dt_start, dt_end).range(self.dataset.frequency.value)
        return [os.path.join(base_path, dt.strftime(self.dataset.path_template)) + "_SUCCESS" for dt in date_times]

    def poke(self, context: Context) -> bool:
        results = []
        for path in self.paths:
            self.log.info("Poking for file %s", path)
            if os.path.isfile(path):
                mod_time = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y%m%d%H%M%S")
                self.log.info("Found File %s last modified: %s", path, mod_time)
                results.append(True)
            else:
                results.append(False)

            for _, _, files in os.walk(path):
                if files:
                    results.append(True)
                else:
                    results.append(False)

        return all(results)

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context=context)
        if not self.poke(context=context):
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=FilesTrigger(
                    paths=self.paths,
                    poke_interval=self.poke_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: bool | None = None) -> None:
        if not event:
            raise AirflowException("%s task failed as no success files found in %s.", self.task_id, self.paths)
        self.log.info("%s completed successfully as all success files found.", self.task_id)