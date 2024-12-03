import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from src.common.enums.tags import Tags
from src.common.sensors.success_sensors import wait_for_dataset

with DAG(
    dag_id="example_data_etl",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    schedule="@hourly",
    tags=[Tags.DRUID]
):
    logical_date = "{{ logical_date }}"

    wait_for_dataset()
    EmptyOperator(task_id="task")