import pendulum
from datetime import timedelta
import json
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

import snowflake.connector

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

local_tz = pendulum.timezone("Asia/Kathmandu")


def write_metrics_to_snowflake(**context):
    """
    Writes one row per DAG run into FOOTBALL_DB.OPS.PIPELINE_RUN_METRICS.

    Expects the loader task (load_incremental_minio_to_snowflake) to print a final JSON line like:
    {"prefix": "...", "files_discovered": ..., "files_to_load": ..., "data_files_loaded": ..., "manifests_loaded": ...}
    and have do_xcom_push=True so Airflow captures it.
    """
    dag_run = context["dag_run"]
    ti = context["ti"]

    # Loader metrics from XCom (last stdout line)
    loader_metrics_raw = ti.xcom_pull(task_ids="load_incremental_minio_to_snowflake")
    loader_metrics = json.loads(loader_metrics_raw) if loader_metrics_raw else {}

    # Durations + state from Airflow task instances
    dbt_run_ti = dag_run.get_task_instance("dbt_run")
    dbt_test_ti = dag_run.get_task_instance("dbt_test")

    dbt_run_duration_sec = dbt_run_ti.duration if dbt_run_ti else None
    dbt_test_duration_sec = dbt_test_ti.duration if dbt_test_ti else None
    dbt_test_state = dbt_test_ti.state if dbt_test_ti else None  # success/failed/upstream_failed/etc.

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )

    try:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS FOOTBALL_DB.OPS")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS FOOTBALL_DB.OPS.PIPELINE_RUN_METRICS (
              DAG_ID STRING,
              RUN_ID STRING,
              EXECUTION_DATE TIMESTAMP_TZ,

              LOADER_PREFIX STRING,
              FILES_DISCOVERED NUMBER,
              FILES_TO_LOAD NUMBER,
              DATA_FILES_LOADED NUMBER,
              MANIFESTS_LOADED NUMBER,

              DBT_RUN_DURATION_SEC NUMBER,
              DBT_TEST_DURATION_SEC NUMBER,
              DBT_TEST_STATE STRING,

              CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )

        cur.execute(
            """
            INSERT INTO FOOTBALL_DB.OPS.PIPELINE_RUN_METRICS (
              DAG_ID, RUN_ID, EXECUTION_DATE,
              LOADER_PREFIX, FILES_DISCOVERED, FILES_TO_LOAD, DATA_FILES_LOADED, MANIFESTS_LOADED,
              DBT_RUN_DURATION_SEC, DBT_TEST_DURATION_SEC, DBT_TEST_STATE
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                dag_run.dag_id,
                dag_run.run_id,
                dag_run.execution_date,
                loader_metrics.get("prefix"),
                loader_metrics.get("files_discovered"),
                loader_metrics.get("files_to_load"),
                loader_metrics.get("data_files_loaded"),
                loader_metrics.get("manifests_loaded"),
                dbt_run_duration_sec,
                dbt_test_duration_sec,
                dbt_test_state,
            ),
        )
        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="football_daily_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 3, 1, tz=local_tz),
    schedule="0 2 * * *",  # 02:00 Nepal time
    catchup=False,
    tags=["football", "minio", "snowflake", "dbt"],
) as dag:

    ingest_incremental = BashOperator(
        task_id="ingest_matches_incremental_to_minio",
        bash_command="cd /opt/airflow/repo && python -m ingestion.src.ingest_matches_incremental",
    )

    load_incremental = BashOperator(
        task_id="load_incremental_minio_to_snowflake",
        bash_command="cd /opt/airflow/repo && python -m ingestion.src.load_incremental_matches_to_snowflake",
        do_xcom_push=True,  # captures final JSON print from loader
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

    write_metrics = PythonOperator(
        task_id="write_run_metrics_to_snowflake",
        python_callable=write_metrics_to_snowflake,
        trigger_rule=TriggerRule.ALL_DONE,  # write metrics even if dbt fails
    )

    ingest_incremental >> load_incremental >> dbt_run >> dbt_test >> write_metrics