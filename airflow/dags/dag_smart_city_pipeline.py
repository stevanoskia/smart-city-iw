"""
Smart City Analytics Pipeline DAG

Hourly ELT pipeline:
  1. Trigger all Airbyte syncs in parallel (one per connection in connection_ids.yml)
  2. Wait for all syncs to complete (XCom job IDs passed via context)
  3. Compile dbt staging (stg_* are ephemeral — inline CTEs, no DB object)
  4. Build + test dbt intermediate (PostgreSQL tables, hourly facts + forecast history)
  5. Build + test dbt marts (star schema: dims + facts + OBT + analytics marts)

dbt packages (dbt_utils) are NOT installed per-run. They live in the `dbt_packages`
named volume (see docker-compose.yml), populated ONCE via a manual `dbt deps` and then
persistent across restarts/rebuilds — so this DAG assumes they're present. If a run
fails with "dbt_utils not found" (e.g. after `docker compose down -v` wiped the volume),
re-run the one-time populate command in the README's Airflow section.

Raw-data retention cleanup lives in the separate smart_city_maintenance DAG
(@daily), decoupled so it runs regardless of any individual ELT run.

Connection IDs loaded from /opt/airflow/ingestion_config/connection_ids.yml
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
from alert_utils import make_success_callback, on_failure

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

def dbt_cmd(select: str, target: str, command: str = "run") -> str:
    return (
        f"{DBT_BIN} {command} --select {select} --target {target} "
        f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} "
        f"--no-partial-parse"
    )

# ── Wait task — pulls job_id from XCom via context ───────────────────────────

def wait_for_sync_xcom(trigger_task_id: str, **context) -> None:
    job_id = context["ti"].xcom_pull(task_ids=trigger_task_id)
    if not job_id:
        raise ValueError(f"No job_id found in XCom from {trigger_task_id}")
    wait_for_sync(str(job_id))

# ── Alert callbacks ───────────────────────────────────────────────────────────
# on_failure is imported from alert_utils and attached to every task via default_args.
# The success email is attached to the LAST task (dbt_marts) only, so it means "the
# whole hourly pipeline — all syncs + intermediate + marts — finished clean", not
# per-task.

notify_success = make_success_callback(
    "Hourly pipeline completed: all syncs + dbt intermediate + marts."
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
    description="Hourly ELT: Airbyte syncs → dbt staging → dbt intermediate → dbt marts",
    schedule_interval="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    # Serialize runs: a run's worst-case duration (wait_syncs 40m + the three dbt
    # steps 15m each) can exceed the hourly interval. Without this, the scheduler
    # would start the next run while the current one is still writing, so two
    # dbt_intermediate/dbt_marts tasks would DELETE+INSERT the same incremental
    # Postgres tables concurrently (deadlocks / lost rows). =1 queues the next run
    # instead; catchup=False means we skip ahead rather than pile up.
    max_active_runs=1,
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
    # dbt packages (dbt_utils) are NOT installed here — they live in the persistent
    # `dbt_packages` named volume (see docker-compose.yml), populated once via a manual
    # `dbt deps`. This keeps a network/registry call off the hourly critical path.

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=dbt_cmd("staging", "staging"),
        execution_timeout=timedelta(minutes=15),
    )

    # ── Step 4: dbt intermediate (PostgreSQL) — build + test ─────────────────
    # `dbt build` runs the models AND their uniqueness/not_null tests, so a
    # duplicate (city, date_utc) fails the pipeline instead of landing silently.

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=dbt_cmd("intermediate", "staging", command="build"),
        execution_timeout=timedelta(minutes=15),
    )

    # ── Step 5: dbt marts (PostgreSQL) — build + test ────────────────────────
    # Star schema (dims + facts), the derived OBT (mart_city_daily), and the
    # analytics marts. `dbt build` runs the relationships/unique/accepted_values
    # tests too, so a broken FK→dimension fails the pipeline. dim_city is derived
    # from the data (no seed), so no `dbt seed` step is needed.

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=dbt_cmd("marts", "staging", command="build"),
        execution_timeout=timedelta(minutes=15),
        on_success_callback=notify_success,   # last task = whole-pipeline success email
    )

    # ── Pipeline order ────────────────────────────────────────────────────────

    trigger_group >> wait_group >> dbt_staging >> dbt_intermediate >> dbt_marts
