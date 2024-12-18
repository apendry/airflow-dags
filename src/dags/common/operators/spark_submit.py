import os.path
from time import time
from uuid import uuid4

from airflow.hooks.filesystem import FSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

from ..datasets import dataset
from ..env import global_config


class BaseSparkSubmitOperator(SparkSubmitOperator):

    def __init__(
            self,
            scala_class,
            application_jar,
            inputs: list[dataset] = None,
            outputs: list[dataset] = None,
            additional_args: list[str] = [],
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
        hook = FSHook(fs_conn_id)
        base_path = hook.get_path() + "/lib"
        self.application_jar = os.path.join(base_path, application_jar)

        self.application_args = additional_args

        if inputs:
            self.application_args += [ for ds in inputs]

        self.conf = global_config.copy()
        if user_conf:
            [self.conf.update({k, v}) for k, v in self.conf]
