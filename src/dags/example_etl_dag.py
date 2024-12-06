import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.datasets import example_dataset
from common.enums.tags import Tags
from common.sensors.success_sensors import DatasetIntervalSensor
from common.datasets import example_metaset
from common.sensors.success_sensors import DatasetLatestSensor

with DAG(
    dag_id="example_data_etl",
    start_date=datetime.datetime(2024, 12, 5, 14),
    end_date=datetime.datetime(2024, 12, 6),
    schedule="@hourly",
    tags=[Tags.DRUID]
):
    logical_date = "{{ logical_date }}"

    start_task = EmptyOperator(task_id="start_task")
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

    end_task = EmptyOperator(task_id="end_task")

    start_task >> [example_data_sensor, example_meta_sensor] >> end_task
