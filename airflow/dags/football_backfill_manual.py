import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

local_tz = pendulum.timezone("Asia/Kathmandu")

with DAG(
    dag_id="football_backfill_manual",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 3, 1, tz=local_tz),
    schedule=None,  # manual trigger only
    catchup=False,
    tags=["football", "backfill", "minio", "snowflake", "dbt"],
) as dag:

    ingest_backfill = BashOperator(
        task_id="ingest_matches_backfill_to_minio",
        bash_command=(
            "cd /opt/airflow/repo && "
            "python -m ingestion.src.ingest_matches_backfill"
        ),
    )

    load_backfill = BashOperator(
        task_id="load_backfill_minio_to_snowflake",
        bash_command=(
            "cd /opt/airflow/repo && "
            "python -m ingestion.src.load_backfill_matches_to_snowflake"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "export DBT_PROFILES_DIR=/opt/airflow/repo/dbt && "
            "cd /opt/airflow/repo/dbt/football_dbt && "
            "dbt run"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "export DBT_PROFILES_DIR=/opt/airflow/repo/dbt && "
            "cd /opt/airflow/repo/dbt/football_dbt && "
            "dbt test"
        ),
    )

    ingest_backfill >> load_backfill >> dbt_run >> dbt_test