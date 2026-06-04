"""
Smart City Analytics Pipeline DAG

Hourly pipeline:
  1. Trigger all 6 Airbyte syncs in parallel
  2. Wait for all syncs to complete (XCom job IDs passed via context)
  3. Run dbt staging (PostgreSQL)
  4. Run dbt warehouse (DuckDB intermediate + marts)
  5. Cleanup old airbyte_raw rows (runs daily at midnight only)

Connection IDs loaded from /opt/airflow/ingestion_config/connection_ids.yml
(mounted from ingestion/config/ in docker-compose.yml).
"""

from __future__ import annotations

import os
import yaml
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

from airbyte_utils import trigger_sync, wait_for_sync

# ── Connection IDs ────────────────────────────────────────────────────────────

CONNECTION_IDS_FILE = Path("/opt/airflow/ingestion_config/connection_ids.yml")
CONNECTION_IDS: dict[str, str] = yaml.safe_load(
    CONNECTION_IDS_FILE.read_text(encoding="utf-8")
)

# ── dbt command template ──────────────────────────────────────────────────────

DBT_PROJECT_DIR  = "/opt/airflow/dbt/smart_city"
DBT_PROFILES_DIR = "/opt/airflow/dbt/smart_city"

# dbt runs from its isolated virtualenv (see airflow/Dockerfile), not Airflow's
# Python env — keeps dbt's protobuf/typing_extensions off Airflow's pins.
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"

def dbt_cmd(select: str, target: str) -> str:
    return (
        f"{DBT_BIN} run --select {select} --target {target} "
        f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} "
        f"--no-partial-parse"
    )

# ── Wait task — pulls job_id from XCom via context ───────────────────────────

def wait_for_sync_xcom(trigger_task_id: str, **context) -> None:
    job_id = context["ti"].xcom_pull(task_ids=trigger_task_id)
    if not job_id:
        raise ValueError(f"No job_id found in XCom from {trigger_task_id}")
    wait_for_sync(str(job_id))

# ── Data retention ────────────────────────────────────────────────────────────

RETENTION_DAYS = {
    "current_weather":   90,
    "air_pollution":     90,
    "weather_forecast":   7,
    "traffic_flow":      30,
    "traffic_incidents": 30,
}

def is_midnight(**context) -> bool:
    """ShortCircuit: only run cleanup at midnight (hour 0)."""
    return context["logical_date"].hour == 0


def cleanup_old_data(**context) -> None:
    """Delete rows from airbyte_raw tables older than their retention window."""
    conn = psycopg2.connect(
        host=os.environ["SMART_CITY_PG_HOST"],
        port=int(os.environ.get("SMART_CITY_PG_PORT", "5432")),
        dbname=os.environ["SMART_CITY_PG_DB"],
        user=os.environ["SMART_CITY_PG_USER"],
        password=os.environ["SMART_CITY_PG_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            for table, days in RETENTION_DAYS.items():
                cur.execute(
                    f"""
                    DELETE FROM airbyte_raw.{table}
                    WHERE _airbyte_extracted_at < NOW() - INTERVAL '{days} days'
                    """,
                )
                deleted = cur.rowcount
                print(f"  {table}: deleted {deleted} rows older than {days} days")
        conn.commit()
        print("Cleanup complete.")
    finally:
        conn.close()

# ── Failure callback ──────────────────────────────────────────────────────────

def on_failure(context) -> None:
    task_id = context["task_instance"].task_id
    dag_id  = context["task_instance"].dag_id
    run_id  = context["run_id"]
    error   = context.get("exception", "unknown error")
    print(
        f"FAILURE | DAG: {dag_id} | Task: {task_id} | Run: {run_id} | Error: {error}"
    )

# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "smart_city",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": on_failure,
    "email_on_failure": False,
}

with DAG(
    dag_id="smart_city_pipeline",
    description="Hourly ELT: Airbyte syncs → dbt staging → dbt warehouse + daily cleanup",
    schedule_interval="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["smart_city", "airbyte", "dbt"],
) as dag:

    # ── Step 1: Trigger all Airbyte syncs in parallel ────────────────────────

    with TaskGroup("trigger_syncs") as trigger_group:
        for name, conn_id in CONNECTION_IDS.items():
            PythonOperator(
                task_id=f"trigger_{name}",
                python_callable=trigger_sync,
                op_args=[conn_id],
                execution_timeout=timedelta(minutes=5),
            )

    # ── Step 2: Wait for all syncs (job IDs pulled from XCom via context) ────

    with TaskGroup("wait_syncs") as wait_group:
        for name in CONNECTION_IDS:
            PythonOperator(
                task_id=f"wait_{name}",
                python_callable=wait_for_sync_xcom,
                op_kwargs={"trigger_task_id": f"trigger_syncs.trigger_{name}"},
                execution_timeout=timedelta(minutes=40),
            )

    # ── Step 3: dbt staging (PostgreSQL) ─────────────────────────────────────

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=dbt_cmd("staging", "staging"),
        execution_timeout=timedelta(minutes=15),
    )

    # ── Step 4: dbt warehouse (DuckDB) ───────────────────────────────────────

    dbt_warehouse = BashOperator(
        task_id="dbt_warehouse",
        bash_command=dbt_cmd("intermediate marts", "warehouse"),
        execution_timeout=timedelta(minutes=15),
    )

    # ── Step 5: Daily cleanup (midnight only) ────────────────────────────────

    midnight_check = ShortCircuitOperator(
        task_id="is_midnight",
        python_callable=is_midnight,
        execution_timeout=timedelta(minutes=1),
    )

    cleanup = PythonOperator(
        task_id="cleanup_old_data",
        python_callable=cleanup_old_data,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Pipeline order ────────────────────────────────────────────────────────

    trigger_group >> wait_group >> dbt_staging >> dbt_warehouse >> midnight_check >> cleanup
