"""
Shared email-alert helpers for the Smart City DAGs.

Both DAGs report failures and successes identically and used to carry copy-pasted copies
of this logic, so every fix had to land twice (the multi-line <pre> rendering below was
one such fix). They now share this module — same import style as airbyte_utils, which the
mounted DAG folder already resolves.

Recipients come from ALERT_EMAIL (set in .env → injected via docker-compose env_file). To
notify more than one person, comma-separate the addresses, e.g.
    ALERT_EMAIL=you@example.com,teammate@example.com
every address gets both the failure and success emails. Unset = the callbacks still run
and log, they just skip the email. SMTP itself is configured via AIRFLOW__SMTP__* env vars.
"""

from __future__ import annotations

import html
import os
from datetime import datetime, timezone

from airflow.utils.email import send_email

ALERT_EMAILS = [e.strip() for e in os.environ.get("ALERT_EMAIL", "").split(",") if e.strip()]

# Render the email timestamp in local time so it matches the inbox clock (Airflow's
# run_id is UTC + the data-interval start, which reads confusingly). Falls back to UTC if
# the container has no tz database.
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


def on_failure(context) -> None:
    """Failure callback for any task — fires once retries are exhausted.

    Emails which step died and why. For a failed Airbyte sync the exception carries the
    origin/type/message and a hint (see airbyte_utils), not just "status: failed".
    """
    task_id = context["task_instance"].task_id
    dag_id  = context["task_instance"].dag_id
    run_id  = context["run_id"]
    error   = context.get("exception", "unknown error")
    print(
        f"FAILURE | DAG: {dag_id} | Task: {task_id} | Run: {run_id} | Error: {error}"
    )
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


def make_success_callback(message: str):
    """Build an on_success_callback that emails `message` when a task succeeds.

    Attach to a DAG's LAST task only, so it means "the whole pipeline finished clean"
    rather than firing per-task.
    """
    def notify_success(context) -> None:
        dag_id = context["task_instance"].dag_id
        run_id = context["run_id"]
        print(f"SUCCESS | DAG: {dag_id} | Run: {run_id} | {message}")
        if ALERT_EMAILS:
            send_email(
                to=ALERT_EMAILS,
                subject=f"[Airflow] {dag_id} SUCCESS",
                html_content=(
                    f"<p><b>DAG:</b> {dag_id}</p>"
                    f"<p><b>Run:</b> {run_id}</p>"
                    f"<p><b>Completed:</b> {_completed_now()}</p>"
                    f"<p>{message}</p>"
                ),
            )

    return notify_success
