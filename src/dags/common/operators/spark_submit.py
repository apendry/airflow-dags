from uuid import uuid4

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def spark_submit_wrapper(scala_class, application_jar, application_args, config: dict = None, task_id = None):

    default_config = {

    }

    [default_config.update({k,v}) for k,v in config]

    SparkSubmitOperator(
        task_id=task_id if task_id else f"spark-submit-{uuid4()}",
        conn_id='spark',
        java_class=scala_class,
        application=application_jar,
        application_args=application_args,
        conf=default_config
    )