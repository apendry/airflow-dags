import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.datasets import example_dataset
from common.enums.tags import Tags
from common.sensors.success_sensors import DatasetSensor

with DAG(
    dag_id="example_data_etl",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    schedule="@hourly",
    tags=[Tags.DRUID]
):
    logical_date = "{{ logical_date }}"

    start_task = EmptyOperator(task_id="start_task")
    example_data_sensor = DatasetSensor(
        task_id="example_data_sensor",
        dataset=example_dataset,
        target_date=logical_date,
        intervals=-1
    )

    start_task >> example_data_sensor
