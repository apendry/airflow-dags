import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from common.datasets import example_dataset
from common.enums.tags import Tags
from common.sensors.success_sensors import DatasetIntervalSensor
from common.datasets import example_metaset
from common.sensors.success_sensors import DatasetLatestSensor
from common.operators.spark_submit import BaseSparkSubmitOperator
from common.operators.dataset_xcom_utils import XcomPushDatasetPaths, xcom_pull_input_paths, xcom_pull_output_path

with DAG(
    dag_id="example_data_etl",
    start_date=datetime.datetime(2024, 12, 5, 14),
    end_date=datetime.datetime(2024, 12, 6),
    schedule="@hourly",
    tags=[Tags.DRUID]
):
    logical_date = "{{ logical_date }}"

    example_data_sensor = DatasetIntervalSensor(
        dataset=example_dataset,
        target_date=logical_date,
        intervals=-1
    )

    example_meta_sensor = DatasetLatestSensor(
        dataset=example_metaset,
        target_date=logical_date,
        intervals=-3
    )

    example_output = XcomPushDatasetPaths(
        dataset=example_dataset,
        target_date=logical_date,
    )

    setup = [example_data_sensor, example_meta_sensor, example_output]

    spark_submit = BaseSparkSubmitOperator(
        scala_class="org.etl.sparkscala.example.ExampleBatchEtl",
        application_jar="/jar/",
        application_args=[
            f"input_data={xcom_pull_input_paths(example_dataset)}",
            f"input_meta={xcom_pull_input_paths(example_metaset)}",
            f"output={xcom_pull_output_path(example_dataset)}"
        ]
    )

    end_task = EmptyOperator(task_id="end_status_check", trigger_rule=TriggerRule.NONE_FAILED)

    setup >> spark_submit >> end_task
