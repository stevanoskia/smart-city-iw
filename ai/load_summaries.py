"""
Step C of the AI city-summaries workflow.

DETERMINISTIC — calls no model API. It takes the generated summaries (from
ai/generate_summaries.py, or from a Claude Code session per ai/PROMPT.md), stamps
the generating model + timestamp, and UPSERTs them into marts.mart_city_summary
(ON CONFLICT on the city_date_key primary key, so re-running a date overwrites
cleanly).

The summary rows only carry (city_date_key, city, date_utc, summary_text); the
star keys city_key/date_key are re-derived from marts.mart_city_daily by joining
on city_date_key, so load_summaries never has to trust hand-copied keys.

CLI (venv313):
    python ai/load_summaries.py [--date YYYY-MM-DD] [--model gemini-2.5-flash]

Requires POSTGRES_* in .env on the host, or SMART_CITY_PG_* in the container.
"""

from __future__ import annotations

import sys
import json
import argparse

from psycopg2.extras import execute_values

try:  # importable both as a module (Airflow DAG) and as a script
    from common import OUTPUTS_DIR, get_conn, load_env, force_utf8_stdout
except ImportError:  # pragma: no cover - direct path import fallback
    from ai.common import OUTPUTS_DIR, get_conn, load_env, force_utf8_stdout

load_env()
force_utf8_stdout()

# Insert from the JSON payload, then fill city_key/date_key from mart_city_daily
# so the star FKs are authoritative (never trust hand-copied keys). Any output row
# whose city_date_key isn't in mart_city_daily is rejected by the join → skipped.
UPSERT = """
insert into marts.mart_city_summary
    (city_date_key, city_key, date_key, city, date_utc, summary_text, model,
     alert_level, alert_headline)
select v.city_date_key, d.city_key, d.date_key, v.city, v.date_utc::date, v.summary_text,
       v.model, v.alert_level, v.alert_headline
from (values %s) as v (city_date_key, city, date_utc, summary_text, model,
                       alert_level, alert_headline)
join marts.mart_city_daily d on d.city_date_key = v.city_date_key
on conflict (city_date_key) do update set
    summary_text   = excluded.summary_text,
    model          = excluded.model,
    alert_level    = excluded.alert_level,
    alert_headline = excluded.alert_headline,
    generated_at   = now()
"""

REQUIRED = {"city_date_key", "city", "date_utc", "summary_text"}


def upsert(rows: list[dict], model: str) -> int:
    """Upsert summary rows; returns the number actually written."""
    if not rows:
        raise ValueError("No summary rows to load.")

    values = []
    for r in rows:
        missing = REQUIRED - r.keys()
        if missing:
            raise ValueError(f"Row {r.get('city', '?')} missing fields: {sorted(missing)}")
        # alert_level/headline are optional so rows written by the manual Claude Code
        # path (which predates them) still load — they just land uncoloured/Normal.
        values.append((r["city_date_key"], r["city"], r["date_utc"],
                       r["summary_text"], model,
                       r.get("alert_level") or "Normal", r.get("alert_headline") or ""))

    conn = get_conn()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        execute_values(cur, UPSERT, values)
        upserted = cur.rowcount
    finally:
        conn.close()

    print(f"Upserted {upserted} of {len(values)} summaries (model={model!r}).")
    if upserted < len(values):
        print(f"  NOTE: {len(values) - upserted} row(s) skipped — city_date_key not "
              f"found in mart_city_daily (stale/mismatched key).")
    return upserted


def resolve_output(requested: str | None):
    if requested:
        return OUTPUTS_DIR / f"{requested}.json"
    files = sorted(OUTPUTS_DIR.glob("*.json"))
    if not files:
        sys.exit(f"No output files in {OUTPUTS_DIR}. Generate summaries first "
                 f"(python ai/generate_summaries.py, or see ai/PROMPT.md).")
    return files[-1]  # latest by date-named filename


def main():
    ap = argparse.ArgumentParser(description="Upsert generated city summaries.")
    ap.add_argument("--date", help="Target date YYYY-MM-DD (default: latest ai/_outputs/*.json)")
    ap.add_argument("--model", default="gemini-3.6-flash",
                    help="Generating model label stored in the model column")
    args = ap.parse_args()

    path = resolve_output(args.date)
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or not rows:
        sys.exit(f"{path} is not a non-empty JSON list.")

    try:
        upsert(rows, args.model)
    except ValueError as exc:
        sys.exit(f"{path.name}: {exc}")


if __name__ == "__main__":
    main()
