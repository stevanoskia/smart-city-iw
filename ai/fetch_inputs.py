"""
Step A of the AI city-summaries workflow.

DETERMINISTIC — calls no model API. It:
  1. Ensures marts.mart_city_summary exists (idempotent DDL).
  2. Resolves a target date (default: latest date in mart_city_daily, or --date).
  3. Reads that date's row + the prior calendar day's row for every city.
  4. Returns / writes a "context pack" — one object per city with its metrics, a
     compact prior-day snapshot for delta framing, and the pre-computed star keys
     (city_date_key, city_key, date_key).

Two consumers:
  * ai/generate_summaries.py (Gemini, scheduled) — imports build_pack() directly,
    or reads the file the CLI writes.
  * a Claude Code session (manual fallback, see ai/PROMPT.md) — reads the file.

CLI (venv313):
    python ai/fetch_inputs.py [--date YYYY-MM-DD]

Requires POSTGRES_* in .env on the host, or SMART_CITY_PG_* in the container.
"""

from __future__ import annotations

import sys
import json
import argparse
from datetime import date, timedelta

from psycopg2.extras import RealDictCursor

try:  # importable both as a module (Airflow DAG) and as a script
    from common import INPUTS_DIR, get_conn, jsonable, load_env, force_utf8_stdout
except ImportError:  # pragma: no cover - direct path import fallback
    from ai.common import INPUTS_DIR, get_conn, jsonable, load_env, force_utf8_stdout

load_env()
force_utf8_stdout()

DDL = """
create table if not exists marts.mart_city_summary (
    city_date_key text primary key,
    city_key      text not null,
    date_key      int  not null,
    city          text not null,
    date_utc      date not null,
    summary_text  text not null,
    model         text not null,
    generated_at  timestamptz not null default now()
);
-- Severity of the day, computed DETERMINISTICALLY in Python (see classify_alerts) —
-- never by the model. Power BI colours the summary page from these, so the colour
-- has to be a property of the data, not of what the model chose to write.
alter table marts.mart_city_summary add column if not exists alert_level    text;
alter table marts.mart_city_summary add column if not exists alert_headline text;
"""

# Metrics pulled for the target day (the full narrative surface).
TARGET_COLS = [
    "city_date_key", "city_key", "date_key", "city", "country", "date_utc",
    "avg_temp_celsius", "min_temp_celsius", "max_temp_celsius", "avg_humidity_pct",
    "total_rain_mm", "dominant_weather_main",
    "avg_aqi", "max_aqi", "avg_pm2_5_ug_m3", "avg_pm10_ug_m3", "hours_poor_air", "aqi_alert",
    "avg_congestion_score", "avg_current_speed_kmh", "total_incidents", "major_incidents",
    "congestion_label",
    "comfort_index", "comfort_index_label", "comfort_trend",
    "rolling_7d_comfort", "prior_7d_comfort",
]

# Compact prior-day snapshot for "vs yesterday" deltas.
PRIOR_COLS = [
    "avg_temp_celsius", "avg_aqi", "avg_pm2_5_ug_m3", "avg_pm10_ug_m3",
    "avg_congestion_score", "total_incidents", "comfort_index",
]


# ── Intraday peaks (hour-level colour for the daily paragraph) ────────────────
# The pipeline only samples PART of each day (Airflow runs while the dev machine is
# on — historically ~07:00-14:00 UTC, with 10 of 24 hours never recorded). So every
# peak is "the peak among observed hours", and the pack carries hours_observed to
# force that framing in the prose. See the coverage warning in summary_spec.md.
PEAK_QUERIES = {
    "warmest":         ("fct_weather_hourly",   "temp_celsius"),
    "worst_aqi":       ("fct_pollution_hourly", "aqi"),
    "worst_congestion": ("fct_traffic_hourly",  "congestion_score"),
}


def _clean(row):
    return {k: jsonable(v) for k, v in row.items()}


def fetch_intraday(cur, target: date) -> dict:
    """Per city: the observed hours and the hour each metric peaked."""
    out: dict[str, dict] = {}

    cur.execute(
        "select city, array_agg(distinct hour_utc order by hour_utc) as hours "
        "from marts.fct_weather_hourly where date_utc = %s group by city",
        (target,),
    )
    for r in cur.fetchall():
        out[r["city"]] = {"hours_observed": r["hours"]}

    for label, (table, col) in PEAK_QUERIES.items():
        # distinct on = the single peak row per city, no window function needed
        cur.execute(
            f"select distinct on (city) city, hour_utc, {col} as value "
            f"from marts.{table} where date_utc = %s and {col} is not null "
            f"order by city, {col} desc, hour_utc",
            (target,),
        )
        for r in cur.fetchall():
            city = out.setdefault(r["city"], {})
            city[f"{label}_hour_utc"] = r["hour_utc"]
            city[f"{label}_value"] = jsonable(r["value"])

    return out


def fetch_alerts(cur, target: date, date_key: int) -> tuple[dict, dict]:
    """Measured pollution alerts ON the day, and weather alerts still AHEAD of it.

    The two are kept apart on purpose. mart_pollution_alerts is measured history, so
    it is fact about the day being summarized. mart_weather_alerts is FORECAST — an
    alert stamped with a date that has already passed says nothing useful in a
    retrospective summary, whereas one for a future date ("severe heat 4-5 Aug") is
    the single most actionable line on the page.
    """
    cur.execute(
        "select city, alert_type, severity, observed_at, trigger_value, trigger_unit "
        "from marts.mart_pollution_alerts where date_key = %s "
        "order by city, severity, observed_at",
        (date_key,),
    )
    pollution: dict[str, list] = {}
    for r in cur.fetchall():
        pollution.setdefault(r["city"], []).append(_clean(r))

    # Collapse the forecast alerts to one row per (city, type, severity) — the raw
    # table repeats a heatwave once per 3-hour forecast slot, which would flood the
    # prompt with 29 near-identical rows.
    cur.execute(
        "select city, alert_type, severity, count(*) as slots, "
        "       min(forecast_at) as first_at, max(forecast_at) as last_at, "
        "       max(trigger_value) as peak_value, max(trigger_unit) as trigger_unit "
        "from marts.mart_weather_alerts where date_key > %s "
        "group by city, alert_type, severity order by city, severity, first_at",
        (date_key,),
    )
    upcoming: dict[str, list] = {}
    for r in cur.fetchall():
        upcoming.setdefault(r["city"], []).append(_clean(r))

    return pollution, upcoming


def classify_alerts(row: dict, pollution: list, upcoming: list) -> tuple[str, str]:
    """Return (alert_level, alert_headline) for a city-day. DETERMINISTIC.

    This drives the red/amber/white colour coding on the Power BI summary page, so
    it is computed from the data here rather than inferred by the model — the colour
    must be reproducible and must agree with the paragraph.
    """
    severe = [a for a in pollution + upcoming if a["severity"] == "Severe"]
    if severe:
        a = severe[0]
        return "Severe", f"Severe {a['alert_type'].lower()}"

    # Warning tier: an explicit alert row, or a metric past its own alert threshold.
    warnings = [a for a in pollution + upcoming if a["severity"] == "Warning"]
    if warnings:
        a = warnings[0]
        where = "forecast" if a in upcoming else "measured"
        return "Warning", f"{a['alert_type']} ({where})"
    if row.get("aqi_alert"):
        return "Warning", "Air-quality alert"
    if (row.get("hours_poor_air") or 0) > 0:
        return "Warning", f"{row['hours_poor_air']}h of poor air"
    if (row.get("max_aqi") or 0) >= 4:
        return "Warning", f"AQI peaked at {row['max_aqi']} (1-5)"
    if row.get("congestion_label") in ("High", "Severe"):
        return "Warning", f"{row['congestion_label']} congestion"
    if row.get("comfort_index_label") == "Poor":
        return "Warning", "Poor comfort index"

    # Explicit text rather than an empty string: a blank cell on the Power BI page
    # reads as "something failed to load", whereas "No alerts" reads as a finding.
    return "Normal", "No alerts"


def resolve_date(cur, requested: str | None) -> date:
    if requested:
        return date.fromisoformat(requested)
    cur.execute("select max(date_utc) from marts.mart_city_daily")
    d = cur.fetchone()["max"]
    if d is None:
        raise RuntimeError("mart_city_daily is empty — nothing to summarize.")
    return d


def build_pack(target_date: str | None = None) -> dict:
    """Ensure the target table exists and return the context pack for a date.

    Raises LookupError if mart_city_daily holds no rows for the resolved date —
    the DAG turns that into a skip (the pipeline simply didn't run that day)
    rather than a failure.
    """
    conn = get_conn()
    conn.autocommit = True
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(DDL)  # ensure target table exists

        target = resolve_date(cur, target_date)
        prior = target - timedelta(days=1)

        cur.execute(
            f"select {', '.join(TARGET_COLS)} from marts.mart_city_daily "
            "where date_utc = %s order by city",
            (target,),
        )
        target_rows = {r["city"]: _clean(r) for r in cur.fetchall()}

        cur.execute(
            f"select city, {', '.join(PRIOR_COLS)} from marts.mart_city_daily "
            "where date_utc = %s",
            (prior,),
        )
        prior_rows = {r["city"]: _clean({k: r[k] for k in PRIOR_COLS}) for r in cur.fetchall()}

        if not target_rows:
            raise LookupError(f"No mart_city_daily rows for {target}.")

        date_key = int(target.strftime("%Y%m%d"))
        intraday = fetch_intraday(cur, target)
        pollution_alerts, upcoming_alerts = fetch_alerts(cur, target, date_key)
    finally:
        conn.close()

    cities = []
    for city, row in target_rows.items():
        pollution = pollution_alerts.get(city, [])
        upcoming = upcoming_alerts.get(city, [])
        level, headline = classify_alerts(row, pollution, upcoming)
        cities.append({
            **row,
            "prior_day": prior_rows.get(city),
            "intraday": intraday.get(city),
            "alerts": {
                "level": level,               # Severe | Warning | Normal
                "headline": headline,         # short, deterministic reason
                "pollution_today": pollution,
                "upcoming_weather": upcoming,
            },
        })

    return {
        "target_date": target.isoformat(),
        "prior_date": prior.isoformat(),
        "cities": cities,
    }


def write_pack(pack: dict) -> "object":
    """Persist a pack to ai/_inputs/<date>.json (the manual path's handoff file)."""
    INPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out = INPUTS_DIR / f"{pack['target_date']}.json"
    out.write_text(json.dumps(pack, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def main():
    ap = argparse.ArgumentParser(description="Build the AI-summary context pack.")
    ap.add_argument("--date", help="Target date YYYY-MM-DD (default: latest in mart_city_daily)")
    args = ap.parse_args()

    try:
        pack = build_pack(args.date)
    except (RuntimeError, LookupError) as exc:
        sys.exit(str(exc))

    out = write_pack(pack)
    levels = {}
    for c in pack["cities"]:
        levels[c["alerts"]["level"]] = levels.get(c["alerts"]["level"], 0) + 1
    print(f"Wrote {out} — {len(pack['cities'])} cities for {pack['target_date']} "
          f"(prior day {pack['prior_date']}). Alert levels: {levels}")


if __name__ == "__main__":
    main()
