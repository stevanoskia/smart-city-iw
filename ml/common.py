"""
Shared plumbing for the ML pipelines.

Same two-environment problem as ai/common.py: these modules run both on the host
(POSTGRES_*, from .env) and inside the Airflow container (SMART_CITY_PG_*, from
docker-compose). get_conn()/get_engine() read whichever pair is present, so the
DAG can import them rather than shelling out.

Deliberately kept as ONE connection helper for all six pipelines — the original
ml/ had five byte-identical copies of get_engine(), which is how the wrong
default port (5434) ended up in all of them at once.
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ML_DIR = Path(__file__).resolve().parent

# Trained models are BUILD ARTIFACTS, not source — gitignored, rebuilt by train.py.
# (The original PR committed 3.4 MB of .joblib into git.)
MODELS_DIR = ML_DIR / "_models"

SCHEMA_FILE = ML_DIR / "schema.sql"


def load_env() -> None:
    """Load the repo-root .env on the host. No-op in the container (no dotenv there)."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(ROOT / ".env")


def _pick(container_var: str, host_var: str, default: str | None = None) -> str | None:
    """SMART_CITY_PG_* wins over POSTGRES_*.

    Order matters, for the same reason documented in ai/common.py: the Airflow
    container gets BOTH (docker-compose sets SMART_CITY_PG_HOST=host.docker.internal,
    but `env_file: ../.env` also drags in POSTGRES_HOST=localhost, which inside a
    container means the container itself).
    """
    return os.getenv(container_var) or os.getenv(host_var) or default


def pg_params() -> dict:
    return {
        "host": _pick("SMART_CITY_PG_HOST", "POSTGRES_HOST", "localhost"),
        "port": int(_pick("SMART_CITY_PG_PORT", "POSTGRES_PORT", "5432")),
        "dbname": _pick("SMART_CITY_PG_DB", "POSTGRES_DB", "smart_city"),
        "user": _pick("SMART_CITY_PG_USER", "POSTGRES_USER", "postgres"),
        "password": _pick("SMART_CITY_PG_PASSWORD", "POSTGRES_PASSWORD"),
    }


def get_conn():
    """Raw psycopg2 connection (writes, DDL)."""
    import psycopg2

    return psycopg2.connect(**pg_params())


def get_engine():
    """SQLAlchemy engine (pandas reads)."""
    from sqlalchemy import create_engine

    p = pg_params()
    return create_engine(
        f"postgresql+psycopg2://{p['user']}:{p['password']}"
        f"@{p['host']}:{p['port']}/{p['dbname']}"
    )


def read_sql(query: str, parse_dates: list[str] | None = None):
    """Run a query into a DataFrame, disposing the engine afterwards."""
    import pandas as pd

    engine = get_engine()
    try:
        return pd.read_sql(query, engine, parse_dates=parse_dates)
    finally:
        engine.dispose()


def ensure_schema() -> None:
    """Apply ml/schema.sql (idempotent).

    Called by train.py and predict.py so a fresh clone does not need a manual
    setup step — the original PR's predict scripts inserted into an ml_predictions
    schema that no file in the repo ever created.
    """
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


# ── Model artifacts ──────────────────────────────────────────────────────────

def model_path(name: str) -> Path:
    return MODELS_DIR / f"{name}.joblib"


def save_bundle(name: str, bundle: dict) -> Path:
    """Persist a trained model plus everything predict() needs to reproduce its inputs."""
    import joblib

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    path = model_path(name)
    joblib.dump(bundle, path)
    return path


def load_bundle(name: str) -> dict:
    import joblib

    path = model_path(name)
    if not path.exists():
        raise FileNotFoundError(
            f"No trained model at {path}. Run:  python ml/train.py --model {name}"
        )
    return joblib.load(path)


# ── City encoding ────────────────────────────────────────────────────────────

def fit_city_codes(cities) -> dict[str, int]:
    """Stable city → int mapping, persisted with the model.

    The original code used `df['city'].astype('category').cat.codes`, which
    renumbers from whatever cities happen to be in THAT dataframe. Train on 10
    cities, predict on a slice holding 9, and every city silently shifts to a
    different code — the model reads the wrong city with no error raised.
    """
    return {city: i for i, city in enumerate(sorted(set(cities)))}


def apply_city_codes(cities, mapping: dict[str, int]):
    """Encode with the training-time mapping; unseen cities → -1."""
    return [mapping.get(c, -1) for c in cities]
