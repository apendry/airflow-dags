import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from common.datasets import example_meta_people
from common.enums.tags import Tags
from common.operators.dataset_xcom_utils import PushOutputDatasetPaths, PullOutputPaths
from common.operators.spark_submit import BaseSparkSubmitOperator
from common.datasets import example_meta_activity

with DAG(
    dag_id="seed_daily_meta",
    start_date=datetime.datetime(2024, 12, 1),
    end_date=datetime.datetime(2024, 12, 31),
    schedule="@daily",
    tags=[Tags.SEED]
):
    logical_date = "{{ logical_date }}"

    start_task = EmptyOperator(task_id="start_bridge_task")

    example_meta_people_output = PushOutputDatasetPaths(
        dataset=example_meta_people,
        target_date=logical_date
    )

    example_meta_activity_output = PushOutputDatasetPaths(
        dataset=example_meta_activity,
        target_date=logical_date
    )

    setup = [example_meta_people_output, example_meta_activity_output]

    spark_submit = BaseSparkSubmitOperator(
        scala_class="org.etl.sparkscala.seed.SeedDataBatch",
        application_jar="/jar/",
        outputs=[
            example_meta_people,
            example_meta_activity
        ]
    )

    end_task = EmptyOperator(task_id="end_status_check", trigger_rule=TriggerRule.NONE_FAILED)

    start_task >> setup >> spark_submit >> end_task
