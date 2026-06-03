"""
Smart City Analytics Pipeline DAG

Hourly pipeline:
  1. Trigger all 6 Airbyte syncs in parallel
  2. Wait for all syncs to complete
  3. Run dbt staging (PostgreSQL)
  4. Run dbt warehouse (DuckDB intermediate + marts)

Connection IDs are loaded from /opt/airflow/ingestion_config/connection_ids.yml
(mounted from ingestion/config/ in docker-compose.yml).
"""

from __future__ import annotations

import yaml
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airbyte_utils import trigger_sync, wait_for_sync

# ── Connection IDs ────────────────────────────────────────────────────────────

CONNECTION_IDS_FILE = Path("/opt/airflow/ingestion_config/connection_ids.yml")
CONNECTION_IDS: dict[str, str] = yaml.safe_load(CONNECTION_IDS_FILE.read_text(encoding="utf-8"))

# ── dbt command template ──────────────────────────────────────────────────────

DBT_PROJECT_DIR = "/opt/airflow/dbt/smart_city"
DBT_PROFILES_DIR = "/opt/airflow/dbt/smart_city"
DBT_BIN = "dbt"

def dbt_cmd(select: str, target: str) -> str:
    return (
        f"{DBT_BIN} run --select {select} --target {target} "
        f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} "
        f"--no-partial-parse"
    )

# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "smart_city",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="smart_city_pipeline",
    description="Hourly ELT: Airbyte syncs → dbt staging → dbt warehouse",
    schedule_interval="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["smart_city", "airbyte", "dbt"],
) as dag:

    # ── Step 1: Trigger all Airbyte syncs in parallel ────────────────────────

    with TaskGroup("trigger_syncs") as trigger_group:
        trigger_tasks = {}
        for name, conn_id in CONNECTION_IDS.items():
            task = PythonOperator(
                task_id=f"trigger_{name}",
                python_callable=trigger_sync,
                op_args=[conn_id],
            )
            trigger_tasks[name] = task

    # ── Step 2: Wait for all syncs to finish ─────────────────────────────────

    with TaskGroup("wait_syncs") as wait_group:
        wait_tasks = {}
        for name in CONNECTION_IDS:
            task = PythonOperator(
                task_id=f"wait_{name}",
                python_callable=wait_for_sync,
                op_args=["{{ ti.xcom_pull(task_ids='trigger_syncs.trigger_" + name + "') }}"],
            )
            wait_tasks[name] = task

    # ── Step 3: dbt staging (PostgreSQL) ─────────────────────────────────────

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=dbt_cmd("staging", "staging"),
    )

    # ── Step 4: dbt warehouse (DuckDB) ───────────────────────────────────────

    dbt_warehouse = BashOperator(
        task_id="dbt_warehouse",
        bash_command=dbt_cmd("intermediate marts", "warehouse"),
    )

    # ── Pipeline order ────────────────────────────────────────────────────────

    trigger_group >> wait_group >> dbt_staging >> dbt_warehouse
