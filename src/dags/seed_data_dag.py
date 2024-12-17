import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from common.datasets import example_meta_people
from common.enums.tags import Tags
from common.operators.dataset_xcom_utils import XcomPushDatasetPaths, xcom_pull_output_path
from common.operators.spark_submit import BaseSparkSubmitOperator
from src.dags.common.datasets import example_meta_activity

with DAG(
    dag_id="seed_daily_meta",
    start_date=datetime.datetime(2024, 12, 1),
    end_date=datetime.datetime(2024, 12, 31),
    schedule="@daily",
    tags=[Tags.SEED]
):
    logical_date = "{{ logical_date }}"

    example_meta_people_output = XcomPushDatasetPaths(
        dataset=example_meta_people,
        target_date=logical_date
    )

    example_meta_activity_output = XcomPushDatasetPaths(
        dataset=example_meta_activity,
        target_date=logical_date
    )

    setup = [example_meta_people_output, example_meta_activity_output]

    spark_submit = BaseSparkSubmitOperator(
        scala_class="org.etl.sparkscala.seed.SeedDataBatch",
        application_jar="/jar/",
        application_args=[
            f"people_meta_output={xcom_pull_output_path(example_meta_people)}",
            f"activity_meta_output={xcom_pull_output_path(example_meta_activity)}"
        ]
    )

    end_task = EmptyOperator(task_id="end_status_check", trigger_rule=TriggerRule.NONE_FAILED)

    setup >> spark_submit >> end_task
