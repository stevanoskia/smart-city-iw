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


def _fmt(dt) -> str:
    """A (UTC, tz-aware) datetime rendered in ALERT_TZ, e.g. '2026-07-20 22:16 CEST'."""
    if dt is None:
        return "—"
    return dt.astimezone(_LOCAL_TZ).strftime("%Y-%m-%d %H:%M %Z")


def _fmt_duration(start, end) -> str:
    """Compact 'Hh Mm Ss' between two datetimes; '' if either is missing or negative."""
    if not start or not end:
        return ""
    secs = int((end - start).total_seconds())
    if secs < 0:
        return ""
    m, s = divmod(secs, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


# The Airflow "logical date" (= data-interval START) is what the UI's "Last Run" column
# shows; for @hourly it's ~1h behind when the run actually executed, so on its own it reads
# as a contradiction next to the completion time (this confused us on a live run). We still
# surface it — it's how you find the run in the UI — but labelled and explained.
_UI_LABEL_NOTE = (
    'Airflow "logical date" = the data-interval start; it\'s what the UI\'s "Last Run" '
    "column shows, ~1h behind the real run time for @hourly."
)


def _run_meta(context) -> dict:
    """The run's identity, pulled from the callback context (guards missing keys)."""
    dag_run = context.get("dag_run")
    return {
        "dag_id":   context["task_instance"].dag_id,
        "run_type": getattr(dag_run, "run_type", None),
        "started":  getattr(dag_run, "start_date", None),
        "logical":  context.get("logical_date") or getattr(dag_run, "logical_date", None),
        "run_id":   context.get("run_id") or getattr(dag_run, "run_id", "—"),
    }


def _run_block_html(context, end_label: str, ended_at, task_id: str | None = None) -> str:
    """Shared run-identity block for both emails.

    Leads with the actual wall-clock run window in ALERT_TZ (Started → <end_label>, with
    duration) so the email answers "when did this run?" at a glance, then the labelled
    logical date so the UI's "Last Run" value reconciles, and finally the raw run_id as a
    small traceability footer (for grepping logs / the CLI) rather than the headline.
    """
    m = _run_meta(context)
    dag_line = html.escape(m["dag_id"]) + (
        f" · {html.escape(str(m['run_type']))} run" if m["run_type"] else ""
    )
    dur = _fmt_duration(m["started"], ended_at)
    end_line = _fmt(ended_at) + (f"  ({dur})" if dur else "")
    task_html = f"<p><b>Task:</b> {html.escape(task_id)}</p>" if task_id else ""
    return (
        f"<p><b>DAG:</b> {dag_line}</p>"
        f"{task_html}"
        f"<p><b>Started:</b> {_fmt(m['started'])}</p>"
        f"<p><b>{html.escape(end_label)}:</b> {end_line}</p>"
        f'<p><b>UI label:</b> {_fmt(m["logical"])} '
        f'<span style="color:#888">({_UI_LABEL_NOTE})</span></p>'
        f'<p style="color:#888"><b>Run id:</b> {html.escape(str(m["run_id"]))}</p>'
    )


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
    m       = _run_meta(context)
    task_id = context["task_instance"].task_id
    error   = context.get("exception", "unknown error")
    print(
        f"FAILURE | DAG: {m['dag_id']} | Task: {task_id} | Run: {m['run_id']} | Error: {error}"
    )
    if ALERT_EMAILS:
        send_email(
            to=ALERT_EMAILS,
            subject=f"[Airflow] {m['dag_id']} FAILED — {task_id}",
            html_content=(
                _run_block_html(context, "Failed", datetime.now(timezone.utc), task_id=task_id)
                + f"<p><b>Error:</b></p>{_error_html(error)}"
            ),
        )


def make_success_callback(message: str):
    """Build an on_success_callback that emails `message` when a task succeeds.

    Attach to a DAG's LAST task only, so it means "the whole pipeline finished clean"
    rather than firing per-task.
    """
    def notify_success(context) -> None:
        m = _run_meta(context)
        print(f"SUCCESS | DAG: {m['dag_id']} | Run: {m['run_id']} | {message}")
        if ALERT_EMAILS:
            send_email(
                to=ALERT_EMAILS,
                subject=f"[Airflow] {m['dag_id']} SUCCESS",
                html_content=(
                    _run_block_html(context, "Finished", datetime.now(timezone.utc))
                    + f"<p>{message}</p>"
                ),
            )

    return notify_success
