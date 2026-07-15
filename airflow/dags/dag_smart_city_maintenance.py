"""
Smart City Maintenance DAG

Daily housekeeping, decoupled from the hourly ELT pipeline:
  - Delete old raw rows (the `staging` schema, written by Airbyte) per the retention policy.

Runs independently of smart_city_pipeline so retention pruning happens regardless
of whether a given ELT run succeeded. Safe to decouple: deduped history is
preserved downstream in the incremental int_city_hourly_* tables, so raw is just
a short buffer (1-day retention >> the hourly models' 6h incremental lookback).
"""

from __future__ import annotations

import html
import os
import psycopg2
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# Email recipients for maintenance alerts (set in .env → injected via docker-compose
# env_file). To notify more than one person, comma-separate the addresses, e.g.
#   ALERT_EMAIL=you@example.com,teammate@example.com
# every address in the list gets both the failure and success emails. Unset =
# callbacks still run and log, they just skip the email. SMTP itself is configured
# via AIRFLOW__SMTP__* env vars (see .env / .env.example).
ALERT_EMAILS = [e.strip() for e in os.environ.get("ALERT_EMAIL", "").split(",") if e.strip()]

# Local-time "Completed" stamp for the email body (Airflow run_id is UTC +
# interval-start, which reads confusingly). Falls back to UTC without tz data.
try:
    from zoneinfo import ZoneInfo
    _LOCAL_TZ = ZoneInfo(os.environ.get("ALERT_TZ", "Europe/Skopje"))
except Exception:
    _LOCAL_TZ = timezone.utc

def _completed_now() -> str:
    return datetime.now(_LOCAL_TZ).strftime("%Y-%m-%d %H:%M %Z")

def _error_html(error) -> str:
    """Render an exception for the alert email, preserving line breaks and escaping HTML."""
    return (
        '<pre style="white-space:pre-wrap;font-family:monospace">'
        f"{html.escape(str(error))}"
        "</pre>"
    )

# ── Data retention ────────────────────────────────────────────────────────────

# Raw is a short buffer, not the archive: deduped hourly history is preserved
# downstream in the incremental int_city_hourly_* tables (which are never pruned).
# Raw only needs to outlive a sync gap so the hourly models never miss an hour.
RETENTION_DAYS = {
    "current_weather":   1,
    "air_pollution":     1,
    "weather_forecast":  1,
    "traffic_flow":      1,
    "traffic_incidents": 1,
}


def cleanup_old_data(**context) -> None:
    """Delete rows from the staging (raw JSON) tables older than their retention window."""
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
                    DELETE FROM staging.{table}
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
    # Fires once retries are exhausted — emails you that the daily raw cleanup failed.
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
# Attached to the cleanup task so it confirms the daily prune ran clean.

def notify_success(context) -> None:
    dag_id = context["task_instance"].dag_id
    run_id = context["run_id"]
    print(f"SUCCESS | DAG: {dag_id} | Run: {run_id} | cleanup completed")
    if ALERT_EMAILS:
        send_email(
            to=ALERT_EMAILS,
            subject=f"[Airflow] {dag_id} SUCCESS",
            html_content=(
                f"<p><b>DAG:</b> {dag_id}</p>"
                f"<p><b>Run:</b> {run_id}</p>"
                f"<p><b>Completed:</b> {_completed_now()}</p>"
                f"<p>Daily staging (raw JSON) cleanup completed.</p>"
            ),
        )

# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "smart_city",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_failure,
    "email_on_failure": False,
}

with DAG(
    dag_id="smart_city_maintenance",
    description="Daily housekeeping: prune old staging (raw JSON) rows per retention policy",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    # Serialize runs so a slow prune can't overlap the next day's — both would
    # DELETE from the same staging tables (and race the pipeline's reads).
    max_active_runs=1,
    default_args=default_args,
    tags=["smart_city", "maintenance"],
) as dag:

    cleanup = PythonOperator(
        task_id="cleanup_old_data",
        python_callable=cleanup_old_data,
        on_success_callback=notify_success,   # confirm the daily prune ran clean
    )
