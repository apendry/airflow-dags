from time import time
from uuid import uuid4

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
            **kwargs
    ):
        super().__init__(
            task_id=task_id if task_id else f"{scala_class.split('.')[-1]}_spark_submit_{time()}",
            conn_id=global_config[Variable.get("env")]["spark"]["conn"],
            java_class=scala_class,
            application_jar=application_jar,
            application_args=application_args,
            **kwargs
        )
        self.task_id = task_id if task_id else f"{scala_class.split('.')[-1]}_spark_submit_{time()}"

        self.conf = global_config.copy()
        if user_conf:
            [self.conf.update({k, v}) for k, v in self.conf]
