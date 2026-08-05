"""
Shared helpers for the AI city-summaries workflow.

The three steps (fetch → generate → load) run in TWO environments with different
env-var names for the same database:

  * host / CLI          → POSTGRES_HOST, POSTGRES_PORT, ... (from .env, like every
                          other host script in this repo)
  * Airflow container   → SMART_CITY_PG_HOST, ... (set in docker-compose.yml)

get_conn() reads whichever pair is present, so the same module works unchanged in
both — the DAG imports these scripts rather than shelling out to them.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from decimal import Decimal
from datetime import date, datetime

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
AI_DIR = Path(__file__).resolve().parent
INPUTS_DIR = AI_DIR / "_inputs"
OUTPUTS_DIR = AI_DIR / "_outputs"

# The generation rules — the single source of truth shared by the Gemini script
# and the manual Claude Code path (ai/PROMPT.md just points at it).
SPEC_FILE = AI_DIR / "summary_spec.md"


def load_env() -> None:
    """Load the repo-root .env when running on the host.

    No-op in the Airflow container: python-dotenv isn't installed there and the
    env vars arrive via docker-compose instead, so a missing dotenv is not an error.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(ROOT / ".env")


def force_utf8_stdout() -> None:
    """Windows console defaults to cp1252 and chokes on µg/m³, °C, em-dashes."""
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass


def get_conn():
    """Connect to the smart_city DB, host or container env var names.

    SMART_CITY_PG_* wins over POSTGRES_*. Order matters: the Airflow container
    gets BOTH — docker-compose sets SMART_CITY_PG_HOST=host.docker.internal, but
    `env_file: ../.env` also drags in the host's POSTGRES_HOST=localhost, which
    inside a container points at the container itself (connection refused). The
    host has no SMART_CITY_PG_* at all, so it falls through to POSTGRES_*.
    """

    def pick(container_var: str, host_var: str, default: str | None = None) -> str | None:
        return os.getenv(container_var) or os.getenv(host_var) or default

    return psycopg2.connect(
        host=pick("SMART_CITY_PG_HOST", "POSTGRES_HOST", "localhost"),
        port=int(pick("SMART_CITY_PG_PORT", "POSTGRES_PORT", "5432")),
        dbname=pick("SMART_CITY_PG_DB", "POSTGRES_DB", "smart_city"),
        user=pick("SMART_CITY_PG_USER", "POSTGRES_USER", "postgres"),
        password=pick("SMART_CITY_PG_PASSWORD", "POSTGRES_PASSWORD"),
    )


def jsonable(v):
    """Make psycopg2 row values JSON-serializable (Decimal → float, dates → ISO)."""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def read_spec() -> str:
    if not SPEC_FILE.exists():
        raise FileNotFoundError(f"Generation spec not found: {SPEC_FILE}")
    return SPEC_FILE.read_text(encoding="utf-8")
