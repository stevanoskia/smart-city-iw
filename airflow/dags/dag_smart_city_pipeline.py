"""
Smart City Analytics Pipeline DAG

Hourly ELT pipeline:
  1. Trigger all Airbyte syncs in parallel (one per connection in connection_ids.yml)
  2. Wait for all syncs to complete (XCom job IDs passed via context)
  3. dbt deps — install pinned dbt packages (dbt_utils) from package-lock.yml
  4. Compile dbt staging (stg_* are ephemeral — inline CTEs, no DB object)
  5. Build + test dbt intermediate (PostgreSQL tables, hourly facts + forecast history)
  6. Build + test dbt marts (star schema: dims + facts + OBT + analytics marts)

Raw-data retention cleanup lives in the separate smart_city_maintenance DAG
(@daily), decoupled so it runs regardless of any individual ELT run.

Connection IDs loaded from /opt/airflow/ingestion_config/connection_ids.yml
(mounted from ingestion/config/ in docker-compose.yml).
"""

from __future__ import annotations

import html
import os
import yaml
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.task_group import TaskGroup

from airbyte_utils import trigger_sync, wait_for_sync

# Email recipients for pipeline alerts (set in .env → injected via docker-compose
# env_file). To notify more than one person, comma-separate the addresses, e.g.
#   ALERT_EMAIL=you@example.com,teammate@example.com
# every address in the list gets both the failure and success emails. Unset =
# callbacks still run and log, they just skip the email. SMTP itself is configured
# via AIRFLOW__SMTP__* env vars.
ALERT_EMAILS = [e.strip() for e in os.environ.get("ALERT_EMAIL", "").split(",") if e.strip()]

# Render the email "Completed" timestamp in local time so it matches the inbox
# clock (Airflow's run_id is UTC + the data-interval start, which reads confusingly).
# Falls back to UTC if the container has no tz database.
try:
    from zoneinfo import ZoneInfo
    _LOCAL_TZ = ZoneInfo(os.environ.get("ALERT_TZ", "Europe/Skopje"))
except Exception:
    _LOCAL_TZ = timezone.utc

def _completed_now() -> str:
    return datetime.now(_LOCAL_TZ).strftime("%Y-%m-%d %H:%M %Z")

def _error_html(error) -> str:
    """Render an exception for the alert email, preserving line breaks.

    Airbyte failures arrive multi-line (origin/type, message, hint — see
    airbyte_utils._describe_failures); a plain <p> collapses them into one run-on, and
    the messages contain characters HTML would eat, hence <pre> + escape.
    """
    return (
        '<pre style="white-space:pre-wrap;font-family:monospace">'
        f"{html.escape(str(error))}"
        "</pre>"
    )

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

def dbt_deps_cmd() -> str:
    # Install the pinned dbt packages (dbt_utils) into the mounted project's
    # dbt_packages/ from the committed package-lock.yml, honoring the exact locked
    # version (1.4.1). Idempotent — a fast no-op when already present.
    return (
        f"{DBT_BIN} deps "
        f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
    )

# ── Wait task — pulls job_id from XCom via context ───────────────────────────

def wait_for_sync_xcom(trigger_task_id: str, **context) -> None:
    job_id = context["ti"].xcom_pull(task_ids=trigger_task_id)
    if not job_id:
        raise ValueError(f"No job_id found in XCom from {trigger_task_id}")
    wait_for_sync(str(job_id))

# ── Failure callback ──────────────────────────────────────────────────────────

def on_failure(context) -> None:
    task_id = context["task_instance"].task_id
    dag_id  = context["task_instance"].dag_id
    run_id  = context["run_id"]
    error   = context.get("exception", "unknown error")
    print(
        f"FAILURE | DAG: {dag_id} | Task: {task_id} | Run: {run_id} | Error: {error}"
    )
    # Fires once retries are exhausted, on ANY task (a sync trigger/wait or a dbt
    # step) — so a failed Airbyte sync emails you which step died and why.
    if ALERT_EMAILS:
        send_email(
            to=ALERT_EMAILS,
            subject=f"[Airflow] {dag_id} FAILED — {task_id}",
            html_content=(
                f"<p><b>DAG:</b> {dag_id}</p>"
                f"<p><b>Task:</b> {task_id}</p>"
                f"<p><b>Run:</b> {run_id}</p>"
                f"<p><b>Failed at:</b> {_completed_now()}</p>"
                f"<p><b>Error:</b></p>{_error_html(error)}"
            ),
        )

# ── Success callback ──────────────────────────────────────────────────────────
# Attached to the LAST task (dbt_marts) only, so it means "the whole hourly
# pipeline — all syncs + intermediate + marts — finished clean", not per-task.

def notify_success(context) -> None:
    dag_id = context["task_instance"].dag_id
    run_id = context["run_id"]
    print(f"SUCCESS | DAG: {dag_id} | Run: {run_id} | pipeline completed")
    if ALERT_EMAILS:
        send_email(
            to=ALERT_EMAILS,
            subject=f"[Airflow] {dag_id} SUCCESS",
            html_content=(
                f"<p><b>DAG:</b> {dag_id}</p>"
                f"<p><b>Run:</b> {run_id}</p>"
                f"<p><b>Completed:</b> {_completed_now()}</p>"
                f"<p>Hourly pipeline completed: all syncs + dbt intermediate + marts.</p>"
            ),
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

    # ── Step 3: dbt deps (install pinned packages) ───────────────────────────
    # Installs dbt_utils (pinned to 1.4.1 via package-lock.yml) into the mounted
    # project's dbt_packages/ before any model runs. Required because dbt_packages/
    # is gitignored AND the dbt project is volume-mounted — so the image can't bake
    # the packages in (the runtime mount would shadow them). Running deps here makes
    # the pipeline self-sufficient instead of relying on a manual host `dbt deps`.

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_deps_cmd(),
        execution_timeout=timedelta(minutes=5),
    )

    # ── Step 4: dbt staging (PostgreSQL) ─────────────────────────────────────

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=dbt_cmd("staging", "staging"),
        execution_timeout=timedelta(minutes=15),
    )

    # ── Step 5: dbt intermediate (PostgreSQL) — build + test ─────────────────
    # `dbt build` runs the models AND their uniqueness/not_null tests, so a
    # duplicate (city, date_utc) fails the pipeline instead of landing silently.

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=dbt_cmd("intermediate", "staging", command="build"),
        execution_timeout=timedelta(minutes=15),
    )

    # ── Step 6: dbt marts (PostgreSQL) — build + test ────────────────────────
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

    trigger_group >> wait_group >> dbt_deps >> dbt_staging >> dbt_intermediate >> dbt_marts
