"""
Smart City AI Summary DAG

Generates one ~60-word, grounded narrative per city per day with the **Gemini API**
and upserts it into marts.mart_city_summary (consumed by the Power BI
"AI City Summaries" page).

Three tasks, mirroring the ai/ scripts:
    fetch_pack  → build the context pack from marts.mart_city_daily (no API call)
    generate    → the model call (Gemini generateContent)
    load        → upsert into marts.mart_city_summary (no API call)

Kept OUT of the hourly smart_city_pipeline on purpose: the summary is daily-grain,
so running it hourly would mean 24 paid model calls to rewrite the same paragraph
— and rewriting a day mid-flight produces a summary of a partial day.

**Which day it summarizes.** `@daily` + `catchup=False` means each run's `ds` is the
day the interval covers — i.e. **yesterday**, a complete UTC day, whose
mart_city_daily row is final. If the dev machine was off at midnight, the scheduler
runs that interval whenever it next comes up (still summarizing the same day), so a
laptop-hosted Airflow doesn't silently skip days. Override with the `date` param
(Trigger DAG w/ config: {"date": "2026-07-29"}) to backfill one specific day.

If the pipeline produced no rows for that date (machine off all day), the run
**skips** rather than fails — a missing day is not a pipeline error, and failing
would send a misleading alert email.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from alert_utils import make_success_callback, on_failure

# The ai/ scripts are bind-mounted (see airflow/docker-compose.yml). They're imported
# rather than shelled out to, so failures surface as real Python tracebacks in the
# task log and the alert email — not an opaque non-zero exit code.
AI_DIR = "/opt/airflow/ai"
if AI_DIR not in sys.path:
    sys.path.append(AI_DIR)

# Label stored in mart_city_summary.model, so a row always says what wrote it
# (rows written by the manual Claude Code fallback say "claude-code").
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")


def _target_date(context) -> str:
    """The day to summarize: the `date` param if given, else the run's data interval."""
    return (context["params"].get("date") or context["ds"])


def fetch_pack(**context) -> dict:
    import fetch_inputs

    target = _target_date(context)
    try:
        pack = fetch_inputs.build_pack(target)
    except LookupError as exc:
        # No marts rows for that day — the ELT pipeline didn't run. Not a failure.
        raise AirflowSkipException(str(exc))

    print(f"Context pack for {pack['target_date']}: {len(pack['cities'])} cities "
          f"(prior day {pack['prior_date']}).")
    return pack  # → XCom (~20 KB of JSON for 10 cities)


def generate(**context) -> list:
    import generate_summaries

    pack = context["ti"].xcom_pull(task_ids="fetch_pack")
    rows = generate_summaries.generate(pack, model=GEMINI_MODEL)

    # Keep the output file too: it's the audit trail of exactly what the model
    # wrote, and it's what the manual load path reads.
    try:
        out = generate_summaries.write_rows(rows, pack["target_date"])
        print(f"Wrote {out}")
    except OSError as exc:  # a read-only mount must not fail an otherwise-good run
        print(f"WARNING: could not write the output file ({exc}) — loading from XCom anyway.")

    return rows


def load(**context) -> None:
    import load_summaries

    rows = context["ti"].xcom_pull(task_ids="generate")
    upserted = load_summaries.upsert(rows, model=GEMINI_MODEL)
    if upserted == 0:
        raise RuntimeError(
            "0 summaries upserted — every city_date_key was rejected by the join "
            "against mart_city_daily. Check that the marts ran for this date."
        )


# ── Alert callbacks ───────────────────────────────────────────────────────────

notify_success = make_success_callback(
    "Daily AI city summaries generated and loaded into marts.mart_city_summary."
)

# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "smart_city",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_failure,
    "email_on_failure": False,
}

with DAG(
    dag_id="smart_city_ai_summary",
    description="Daily AI-generated city narratives (Gemini) → marts.mart_city_summary",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    # Serialize: two runs would upsert the same city_date_key rows concurrently
    # and burn double the API quota for the same output.
    max_active_runs=1,
    default_args=default_args,
    params={"date": None},  # optional YYYY-MM-DD override for backfilling one day
    tags=["smart_city", "ai"],
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_pack",
        python_callable=fetch_pack,
    )

    t_generate = PythonOperator(
        task_id="generate",
        python_callable=generate,
        # The model call is the only task that touches the network/quota; its own
        # backoff handles 429s, and these retries cover a longer outage.
        execution_timeout=timedelta(minutes=10),
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
        on_success_callback=notify_success,  # last task = whole-DAG success
    )

    t_fetch >> t_generate >> t_load
