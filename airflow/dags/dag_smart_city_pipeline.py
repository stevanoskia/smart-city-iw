"""
Smart City Analytics Pipeline DAG

Hourly ELT pipeline:
  1. Reconcile Airbyte with the metadata config (config.sources/streams/source_locations)
     so new sources/cities are applied automatically — best-effort, never blocks ingestion
     (see setup_airbyte.reconcile)
  2. Sync all Airbyte connections in parallel — one task per connection triggers its
     sync AND waits for it, so an Airflow retry re-triggers a fresh sync (not a dead job)
  3. Validate the data contract against the metadata config (config.field_mappings +
     config.validation_rules) — stop before dbt if a required field is missing/all-NULL
     or an error-severity quality threshold is breached; log every check to
     config.validation_runs (see config_utils.validate_streams)
  4. Compile dbt staging (stg_* are ephemeral — inline CTEs, no DB object; built from
     config.field_mappings via the build_staging macro)
  5. Build + test dbt intermediate (PostgreSQL tables, hourly facts + forecast history)
  6. Build + test dbt marts (star schema: dims + facts + OBT + analytics marts)

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
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airbyte_utils import trigger_sync, wait_for_sync
from alert_utils import make_success_callback, on_failure
from config_utils import validate_streams

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

# ── Auto-detect: apply config.* changes to Airbyte (STEP 04) ──────────────────
# Runs setup_airbyte.reconcile() so new sources/cities added to config.* are pushed to
# Airbyte before the syncs — the engine "picks up new config automatically". BEST-EFFORT
# on purpose: the import is done INSIDE the task (a broken import can't break DAG parsing)
# and it never raises (a hiccup can't block ingestion — a real Airbyte outage surfaces on
# the richer sync tasks instead). It skips the destination (LAN-IP detection is host-only).
# New *cities* on an existing source are picked up by that source's next sync immediately;
# a brand-new *source* still needs a DAG re-parse before it gains a sync task.

def run_reconcile_airbyte(**context) -> None:
    import sys
    sys.path.insert(0, "/opt/airflow/ingestion_scripts")
    try:
        import setup_airbyte
        connection_ids = setup_airbyte.reconcile()
        print(f"reconcile_airbyte: applied config to Airbyte ({len(connection_ids)} connection(s)).")
    except Exception as e:
        print(f"reconcile_airbyte: SKIPPED (best-effort) — {type(e).__name__}: {e}")
        context["ti"].xcom_push(key="reconcile_error", value=str(e))

# ── Sync task — trigger + wait in ONE task so a retry re-triggers ─────────────
# Trigger and wait used to be two tasks (trigger pushed the job_id to XCom, wait polled
# it). That made retries useless: a failed sync only fails the *wait* task, whose retry
# re-pulls the SAME job_id and re-polls a job that already failed — a dead job never
# comes back, so it can never recover (e.g. after the destination is re-pointed to a new
# LAN IP). Trigger + wait in one task means an Airflow retry re-triggers a *fresh* sync.
# trigger_sync's 409 handling still attaches to an already-running job if a prior attempt
# left one live, so a retry won't double-trigger.

def sync_connection(connection_id: str) -> None:
    job_id = trigger_sync(connection_id)
    wait_for_sync(job_id)

# ── Data-contract validation gate (STEP 05: Monitor & Validate) ───────────────
# Reads the metadata config (config.field_mappings required + config.validation_rules)
# and checks the latest raw batch each sync just wrote, BEFORE dbt runs — so bad or
# missing data never reaches intermediate/marts. Every check (pass and fail) is logged
# to config.validation_runs (committed immediately, so the reason a run stopped is always
# queryable), and any error-severity breach raises AirflowException — which the on_failure
# callback (attached via default_args) turns into an alert email naming exactly what failed.

def run_contract_validation(**context) -> None:
    result = validate_streams(airflow_run_id=context.get("run_id"))
    warnings, failures, certified = result["warnings"], result["failures"], result["certified"]
    for w in warnings:
        print(f"WARN  {w}")
    if warnings:
        context["ti"].xcom_push(key="validation_warnings", value=len(warnings))
    if failures:
        raise AirflowException(
            "Data-contract validation FAILED — pipeline stopped before dbt:\n"
            + "\n".join(f"  - {f}" for f in failures)
        )
    print(f"Contract OK — certified: {', '.join(certified) or 'none'}; warnings: {len(warnings)}")

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
    # Serialize runs: a run's worst-case duration (syncs 45m + the three dbt
    # steps 15m each) can exceed the hourly interval. Without this, the scheduler
    # would start the next run while the current one is still writing, so two
    # dbt_intermediate/dbt_marts tasks would DELETE+INSERT the same incremental
    # Postgres tables concurrently (deadlocks / lost rows). =1 queues the next run
    # instead; catchup=False means we skip ahead rather than pile up.
    max_active_runs=1,
    default_args=default_args,
    tags=["smart_city", "airbyte", "dbt"],
) as dag:

    # ── Step 1: Auto-detect — reconcile Airbyte with config.* (best-effort) ───
    # Applies new sources/cities from config.* to Airbyte before syncing. Never blocks
    # ingestion (the callable swallows errors); real Airbyte outages surface on the syncs.

    reconcile_airbyte = PythonOperator(
        task_id="reconcile_airbyte",
        python_callable=run_reconcile_airbyte,
        retries=1,
        execution_timeout=timedelta(minutes=5),
    )

    # ── Step 2: Sync all Airbyte connections in parallel (trigger + wait) ─────
    # One task per connection triggers its sync and waits for it. All connections
    # still sync concurrently (the group runs in parallel); merging trigger+wait only
    # removes the XCom hop and makes a retry re-trigger instead of re-polling a dead job.

    with TaskGroup("syncs") as sync_group:
        for name, conn_id in CONNECTION_IDS.items():
            PythonOperator(
                task_id=f"sync_{name}",
                python_callable=sync_connection,
                op_args=[conn_id],
                execution_timeout=timedelta(minutes=45),  # trigger (secs) + wait (≤35m)
            )

    # ── Step 3: Data-contract validation gate ────────────────────────────────
    # Stops the pipeline if a required field is missing/all-NULL or an error-severity
    # quality threshold is breached (config.validation_rules). retries=0 overrides the
    # DAG default: a contract failure isn't transient (re-reading the same batch fails
    # identically), so the alert fires immediately instead of after ~15 min of backoff.

    validate_contract = PythonOperator(
        task_id="validate_contract",
        python_callable=run_contract_validation,
        retries=0,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Step 4: dbt staging (PostgreSQL) ─────────────────────────────────────
    # dbt packages (dbt_utils) are NOT installed here — they live in the persistent
    # `dbt_packages` named volume (see docker-compose.yml), populated once via a manual
    # `dbt deps`. This keeps a network/registry call off the hourly critical path.

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

    reconcile_airbyte >> sync_group >> validate_contract >> dbt_staging >> dbt_intermediate >> dbt_marts
