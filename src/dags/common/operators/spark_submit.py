import os.path
from time import time
from uuid import uuid4

from airflow.hooks.filesystem import FSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

from ..env import global_config


class BaseSparkSubmitOperator(SparkSubmitOperator):

    def __init__(
            self,
            scala_class,
            application_jar,
            application_args,
            user_conf: dict = None,
            task_id=None,
            fs_conn_id="fs_default",
            **kwargs
    ):
        super().__init__(
            task_id=task_id if task_id else f"{scala_class.split('.')[-1]}_spark_submit_{time()}",
            conn_id=global_config[Variable.get("env")]["spark"]["spark_conn"],
            java_class=scala_class,
            application_args=application_args,
            **kwargs
        )
        hook = FSHook(fs_conn_id)
        base_path = hook.get_path() + "/lib"
        self.application_jar = os.path.join(base_path, application_jar)
        self.task_id = task_id if task_id else f"{scala_class.split('.')[-1]}_spark_submit_{time()}"

        self.conf = global_config.copy()
        if user_conf:
            [self.conf.update({k, v}) for k, v in self.conf]
