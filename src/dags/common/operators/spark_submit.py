import os.path
import logging

from airflow.hooks.filesystem import FSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.utils.context import Context

from ..datasets import dataset
from ..enums.xcom import TaskIds, Keys
from ..env import global_config


class BaseSparkSubmitOperator(SparkSubmitOperator):

    def __init__(
            self,
            scala_class,
            application_jar,
            inputs: list[dataset] = None,
            outputs: list[dataset] = None,
            additional_args: list[str] = None,
            user_conf: dict = None,
            task_id=None,
            fs_conn_id="fs_default",
            **kwargs
    ):
        self.task_id = task_id if task_id else f"{scala_class.split('.')[-1]}_spark_submit".lower()
        super().__init__(
            task_id=self.task_id,
            conn_id=global_config[Variable.get("env")]["spark"]["spark_conn"],
            java_class=scala_class,
            **kwargs
        )
        self.inputs = inputs
        self.outputs = outputs
        self.fs_conn_id = fs_conn_id
        self.application_jar = application_jar
        self.application_args = additional_args if additional_args else []

        self.conf = global_config.copy()
        if user_conf:
            [self.conf.update({k, v}) for k, v in self.conf]

    def execute(self, context: Context) -> None:

        hook = FSHook(self.fs_conn_id)
        base_path = hook.get_path() + "/lib"
        self.application = os.path.join(base_path, self.application_jar)

        if self.inputs:
            self.application_args += [
                f"--input-{ds.name.lower()}={self.xcom_pull(context=context,task_ids=TaskIds.input(ds),key=Keys.input(ds))}" for ds in self.inputs
            ]

        if self.outputs:
            self.application_args += [
                f"--output-{ds.name.lower()}={self.xcom_pull(context=context,task_ids=TaskIds.output(ds),key=Keys.output(ds))}" for ds in self.outputs
            ]

        super().execute(context)
