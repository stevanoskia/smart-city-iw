"""
Smart City ML DAG

Retrains the six prediction pipelines and writes their forecasts into
ml_predictions.*, daily.

    train  → predict  → report

WHY A SEPARATE PYTHON. The pipelines run in /home/airflow/ml_venv, not in the
scheduler's own environment — airflow 2.9.3 pins pandas and numpy, and pulling
scikit-learn/xgboost into that env restarts the dependency tug-of-war that
already forced dbt into its own venv (see airflow/Dockerfile). So these tasks
shell out, exactly like the dbt tasks in the hourly DAG do. The cost is that a
failure arrives as a non-zero exit code rather than a Python traceback, so
_run() captures the child's output and puts the tail of it in the raised
exception — which is what the alert email renders.

WHY TRAIN EVERY DAY. Trained models are build artifacts, not source (the original
ML commit checked 3.4 MB of .joblib into git). Rather than solve artifact
distribution — a registry, or another named volume like dbt_packages — the whole
training set is currently ~1,700 rows and trains in seconds, so retraining each
run is cheaper than storing anything, and the models always see the newest data.
If training ever costs real time, split this into a weekly train DAG and a daily
predict DAG sharing a volume.

WHY IT IS NOT PART OF THE HOURLY PIPELINE. The horizons are 1h–1d and the
city-score model is daily-grain; running hourly would rewrite the same forecasts
24 times a day off partial data, for no new information.

FAILING MODELS DO NOT FAIL THE DAG. train.py exits non-zero when a model loses to
its naive baseline — right for a human at a terminal, wrong for a scheduler.
"24h PM2.5 is genuinely hard on seven weeks of gappy data" is a standing property
of the data, not an incident, and mailing it daily would train everyone to ignore
the alerts. The DAG passes --allow-worse-than-baseline and surfaces skill through
ml_predictions.model_health instead; the `report` task logs the standings so a
real regression is still visible in the run log.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from alert_utils import make_success_callback, on_failure

# ml/ is bind-mounted (see airflow/docker-compose.yml); ml_venv is built into the
# image (see airflow/Dockerfile).
ML_DIR = "/opt/airflow/ml"
ML_PYTHON = "/home/airflow/ml_venv/bin/python"

# How much of the child's output to keep in the exception message. Enough to show
# which pipeline died and why, short enough that the alert email stays readable.
ERROR_TAIL_LINES = 25


def _run(script: str, *args: str) -> str:
    """Run one ml/ script in ml_venv, streaming its output into the task log.

    Raises AirflowException carrying the tail of the output on failure, so the
    on_failure alert email says what actually broke instead of just "exit 1".
    """
    cmd = [ML_PYTHON, f"{ML_DIR}/{script}", *args]
    print("$ " + " ".join(cmd))

    proc = subprocess.run(
        cmd,
        cwd=ML_DIR,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    print(output)

    if proc.returncode != 0:
        tail = "\n".join(output.strip().splitlines()[-ERROR_TAIL_LINES:])
        raise AirflowException(
            f"{script} exited with code {proc.returncode}.\n\n{tail}"
        )
    return output


def train(**_) -> None:
    # --allow-worse-than-baseline: see the module docstring. The verdict is still
    # recorded per model in ml_predictions.model_registry.
    _run("train.py", "--allow-worse-than-baseline")


def predict(**_) -> None:
    output = _run("predict.py")
    # predict.py exits 0 when it wrote nothing but had a coherent reason (no
    # trained model yet, no source rows). Writing zero rows on a scheduled run is
    # still worth failing on — it means the forecasts silently went stale.
    if "wrote 0 rows" in output:
        raise AirflowException(
            "predict.py wrote 0 rows into ml_predictions.* — every pipeline was "
            "skipped. Check the train task's log for untrained models."
        )


def report(**_) -> None:
    """Log current model skill so a regression shows up in the run log.

    Runs in Airflow's own env rather than ml_venv: it is a plain psycopg2 query
    against a view, with no pandas/sklearn involved.
    """
    import psycopg2

    import os

    conn = psycopg2.connect(
        host=os.getenv("SMART_CITY_PG_HOST", "host.docker.internal"),
        port=int(os.getenv("SMART_CITY_PG_PORT", "5432")),
        dbname=os.getenv("SMART_CITY_PG_DB", "smart_city"),
        user=os.getenv("SMART_CITY_PG_USER", "postgres"),
        password=os.getenv("SMART_CITY_PG_PASSWORD"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select model_name, horizon, metric_name, metric_value,
                       baseline_value, skill, beats_baseline, rows_trained
                from ml_predictions.model_health
                order by beats_baseline, model_name
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print("model_registry is empty — no training run has been recorded yet.")
        return

    print(f"{'model':<14}{'horizon':<9}{'metric':<9}{'value':>10}{'baseline':>10}"
          f"{'skill':>9}   verdict")
    for name, horizon, metric, value, base, skill, beats, n in rows:
        v = f"{value:10.4f}" if value is not None else f"{'n/a':>10}"
        b = f"{base:10.4f}" if base is not None else f"{'n/a':>10}"
        s = f"{skill:+8.1%}" if skill is not None else f"{'n/a':>8}"
        print(f"{name:<14}{horizon:<9}{metric:<9}{v}{b}{s}   "
              f"{'PASS' if beats else 'below baseline'}  (n={n})")

    losing = [r[0] for r in rows if not r[6]]
    if losing:
        print(f"\nBelow their naive baseline: {', '.join(losing)}. Predictions are "
              f"still written and labelled — treat those as weak, not authoritative.")


notify_success = make_success_callback(
    "Daily ML pipelines retrained and forecasts written to ml_predictions.*"
)

default_args = {
    "owner": "smart_city",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": on_failure,
    "email_on_failure": False,
}

with DAG(
    dag_id="smart_city_ml",
    description="Daily ML forecasts (AQI, temperature, traffic, rain, city score, anomaly)",
    schedule_interval="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    # Two concurrent runs would fit the same models and upsert the same keys.
    max_active_runs=1,
    default_args=default_args,
    tags=["smart_city", "ml"],
) as dag:

    t_train = PythonOperator(task_id="train", python_callable=train)
    t_predict = PythonOperator(task_id="predict", python_callable=predict)
    t_report = PythonOperator(
        task_id="report",
        python_callable=report,
        on_success_callback=notify_success,  # last task = whole-DAG success
    )

    t_train >> t_predict >> t_report
