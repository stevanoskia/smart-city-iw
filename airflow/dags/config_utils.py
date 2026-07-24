"""
Config-driven data-contract validation for the Smart City pipeline.

Reads the metadata `config` schema (see metadata/schema.sql) and validates the
latest raw batch each stream just synced into `staging.*`, BEFORE dbt runs — so
bad/missing data never reaches the intermediate/marts layers.

Two tiers of checks, both config-driven:
  1. Presence  — every active + required `config.field_mappings` row must be
     present (its source_expr evaluates) and not entirely NULL in the batch.
  2. Thresholds — every active `config.validation_rules` row (min/max/
     accepted_values/max_null_pct/min_row_count/freshness_minutes). severity
     'error' stops the pipeline; 'warn' only logs.

Every check (pass and fail) is written to `config.validation_runs` under
autocommit, so the audit row persists even though the DAG task then raises. This
module is Airflow-agnostic (pure psycopg2); the DAG task calls validate_streams()
and raises AirflowException on failures.

Connects with the same SMART_CITY_PG_* env vars the maintenance DAG uses.
"""

from __future__ import annotations

import json
import os

import psycopg2


# ── Connection ────────────────────────────────────────────────────────────────

def get_conn(autocommit: bool = True):
    """psycopg2 connection using the SMART_CITY_PG_* env (as dag_smart_city_maintenance)."""
    conn = psycopg2.connect(
        host=os.environ["SMART_CITY_PG_HOST"],
        port=int(os.environ.get("SMART_CITY_PG_PORT", "5432")),
        dbname=os.environ["SMART_CITY_PG_DB"],
        user=os.environ["SMART_CITY_PG_USER"],
        password=os.environ["SMART_CITY_PG_PASSWORD"],
    )
    conn.autocommit = autocommit
    return conn


# ── Config reads ──────────────────────────────────────────────────────────────

def active_streams(conn) -> list[dict]:
    """Active streams whose source is also active."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select st.stream_id, st.stream_name, st.target_schema, st.target_table
            from config.streams st
            join config.sources s on s.source_id = st.source_id
            where st.is_active and s.is_active
            order by st.stream_name
            """
        )
        cols = ("stream_id", "stream_name", "target_schema", "target_table")
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def active_fields(conn, stream_id: int) -> list[dict]:
    """All active field mappings for a stream (source_expr lookup + required subset)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select target_column, source_expr, is_required
            from config.field_mappings
            where stream_id = %s and is_active
            order by ordinal
            """,
            (stream_id,),
        )
        cols = ("target_column", "source_expr", "is_required")
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def all_field_columns(conn, stream_id: int) -> set[str]:
    """Every field target_column for a stream (active OR inactive) — used to tell a
    legitimately-disabled field from a typo in a validation_rules.target_column."""
    with conn.cursor() as cur:
        cur.execute(
            "select target_column from config.field_mappings where stream_id = %s",
            (stream_id,),
        )
        return {r[0] for r in cur.fetchall()}


def active_rules(conn, stream_id: int) -> list[dict]:
    """Active validation rules for a stream."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select target_column, rule_type, rule_value, severity, coalesce(description, '')
            from config.validation_rules
            where stream_id = %s and is_active
            order by target_column nulls first, rule_type
            """,
            (stream_id,),
        )
        cols = ("target_column", "rule_type", "rule_value", "severity", "description")
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── Audit log ─────────────────────────────────────────────────────────────────

def _log(conn, airflow_run_id, stream_name, target_column, check_type,
         status, rows_checked=None, null_count=None, detail=None) -> None:
    """Insert one validation_runs row (committed immediately under autocommit)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into config.validation_runs
                (airflow_run_id, stream_name, target_column, check_type,
                 status, rows_checked, null_count, detail)
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (airflow_run_id, stream_name, target_column, check_type,
             status, rows_checked, null_count, detail),
        )


# ── Low-level checks against the latest raw batch ─────────────────────────────
# The latest sync's rows = those within a short window of the newest extract.
# full_refresh_append re-appends every record each sync, so this isolates the
# most recent batch from prior hours' rows.

def _window(schema: str, table: str, minutes: int) -> str:
    return (
        f"_airbyte_extracted_at >= "
        f"(select max(_airbyte_extracted_at) - interval '{minutes} minutes' "
        f"from {schema}.{table})"
    )


def _batch_rows(conn, schema, table, window) -> int:
    with conn.cursor() as cur:
        cur.execute(f"select count(*) from {schema}.{table} where {window}")
        return cur.fetchone()[0]


def _non_null(conn, schema, table, expr, window) -> int:
    with conn.cursor() as cur:
        cur.execute(f"select count({expr}) from {schema}.{table} where {window}")
        return cur.fetchone()[0]


def _freshness_minutes(conn, schema, table) -> float | None:
    with conn.cursor() as cur:
        cur.execute(
            f"select extract(epoch from (now() - max(_airbyte_extracted_at))) / 60.0 "
            f"from {schema}.{table}"
        )
        val = cur.fetchone()[0]
        return float(val) if val is not None else None


def _count_violations(conn, schema, table, where_expr, window, params=()) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"select count(*) from {schema}.{table} where {window} and ({where_expr})",
            params,
        )
        return cur.fetchone()[0]


# ── Rule evaluation ───────────────────────────────────────────────────────────
# Returns (breached: bool, detail: str). Raises on malformed rules/exprs; the
# caller records that as a failed check rather than crashing the whole task.

def _eval_rule(conn, schema, table, rule, exprs, window, rows_checked):
    rtype = rule["rule_type"]
    col = rule["target_column"]
    val = rule["rule_value"]
    label = f"{table}" + (f".{col}" if col else "")

    if rtype == "min_row_count":
        need = int(val)
        if rows_checked < need:
            return True, f"{label}: {rows_checked} rows < required {need}"
        return False, ""

    if rtype == "freshness_minutes":
        limit = float(val)
        age = _freshness_minutes(conn, schema, table)
        if age is None:
            return True, f"{label}: no rows, cannot assess freshness"
        if age > limit:
            return True, f"{label}: newest data {age:.0f} min old > limit {limit:.0f} min"
        return False, ""

    # Column-level rules need the field's source_expr.
    expr = exprs.get(col)
    if expr is None:
        return False, ""  # field inactive/unknown — nothing to check

    if rows_checked == 0:
        return False, ""  # no rows to test (min_row_count owns emptiness)

    if rtype in ("min", "max"):
        op = "<" if rtype == "min" else ">"
        n = _count_violations(
            conn, schema, table,
            f"({expr}) is not null and ({expr})::numeric {op} (%s)::numeric",
            window, (val,),
        )
        if n:
            return True, f"{label}: {n} value(s) {op} {val}"
        return False, ""

    if rtype == "not_null":
        non_null = _non_null(conn, schema, table, expr, window)
        nulls = rows_checked - non_null
        if nulls:
            return True, f"{label}: {nulls}/{rows_checked} NULL"
        return False, ""

    if rtype == "max_null_pct":
        non_null = _non_null(conn, schema, table, expr, window)
        pct = 100.0 * (rows_checked - non_null) / rows_checked
        if pct > float(val):
            return True, f"{label}: {pct:.0f}% NULL > limit {val}%"
        return False, ""

    if rtype == "accepted_values":
        allowed = [str(v) for v in json.loads(val)]
        n = _count_violations(
            conn, schema, table,
            f"({expr}) is not null and ({expr})::text <> all(%s)",
            window, (allowed,),
        )
        if n:
            return True, f"{label}: {n} value(s) outside {allowed}"
        return False, ""

    raise ValueError(f"unknown rule_type '{rtype}'")


# ── Orchestrator ──────────────────────────────────────────────────────────────

def validate_streams(airflow_run_id: str | None = None, batch_window_minutes: int = 5) -> dict:
    """Validate every active stream's latest raw batch.

    Returns {'failures': [...], 'warnings': [...], 'certified': [...]}. Writes a
    config.validation_runs row per check (committed immediately). Does NOT raise —
    the DAG task inspects 'failures' and raises AirflowException so the message
    flows into the existing on_failure alert email.
    """
    conn = get_conn(autocommit=True)
    failures: list[str] = []
    warnings: list[str] = []
    certified: list[str] = []
    try:
        for stream in active_streams(conn):
            sname = stream["stream_name"]
            schema = stream["target_schema"]
            table = stream["target_table"]
            window = _window(schema, table, batch_window_minutes)
            stream_errored = False

            # rows in the latest batch (also catches a missing staging table)
            try:
                rows_checked = _batch_rows(conn, schema, table, window)
            except Exception as e:
                detail = f"{table}: cannot read staging table ({e}); has the sync run?"
                _log(conn, airflow_run_id, sname, None, "batch", "missing", detail=detail)
                failures.append(detail)
                continue

            fields = active_fields(conn, stream["stream_id"])
            exprs = {f["target_column"]: f["source_expr"] for f in fields}
            known_cols = all_field_columns(conn, stream["stream_id"])

            # Tier 1 — required-field presence
            for f in (f for f in fields if f["is_required"]):
                col, expr = f["target_column"], f["source_expr"]
                try:
                    non_null = _non_null(conn, schema, table, expr, window)
                except Exception as e:
                    detail = f"{table}.{col}: required field missing/invalid ({e})"
                    _log(conn, airflow_run_id, sname, col, "required", "missing",
                         rows_checked=rows_checked, detail=detail)
                    failures.append(detail)
                    stream_errored = True
                    continue
                if rows_checked > 0 and non_null == 0:
                    detail = f"{table}.{col}: required field entirely NULL in {rows_checked} rows"
                    _log(conn, airflow_run_id, sname, col, "required", "null",
                         rows_checked=rows_checked, null_count=rows_checked, detail=detail)
                    failures.append(detail)
                    stream_errored = True
                else:
                    _log(conn, airflow_run_id, sname, col, "required", "ok",
                         rows_checked=rows_checked, null_count=(rows_checked - non_null))

            # Tier 2 — quality-threshold rules
            for rule in active_rules(conn, stream["stream_id"]):
                rtype, sev, col = rule["rule_type"], rule["severity"], rule["target_column"]
                # Config sanity: a column-level rule whose target_column isn't a known field
                # (a typo — target_column is free text, not an FK) would silently never fire.
                # Surface it as a non-blocking warning instead of a silent no-op. A known-but-
                # inactive field is legitimate (temporarily disabled) and stays quiet.
                if col is not None and col not in known_cols:
                    detail = f"{table}: rule '{rtype}' targets unknown column '{col}' (typo?) - rule skipped"
                    _log(conn, airflow_run_id, sname, col, rtype, "config_warning", detail=detail)
                    warnings.append(detail)
                    continue
                try:
                    breached, detail = _eval_rule(
                        conn, schema, table, rule, exprs, window, rows_checked
                    )
                except Exception as e:
                    detail = f"{table}: rule '{rtype}' failed to evaluate ({e})"
                    _log(conn, airflow_run_id, sname, col, rtype, "missing",
                         rows_checked=rows_checked, detail=detail)
                    failures.append(detail)
                    stream_errored = True
                    continue
                if breached:
                    _log(conn, airflow_run_id, sname, col, rtype, "below_threshold",
                         rows_checked=rows_checked, detail=detail)
                    if sev == "error":
                        failures.append(detail)
                        stream_errored = True
                    else:
                        warnings.append(detail)
                else:
                    _log(conn, airflow_run_id, sname, col, rtype, "ok",
                         rows_checked=rows_checked)

            # Certify the stream if nothing error-level touched it
            if not stream_errored:
                certified.append(sname)
                _log(conn, airflow_run_id, sname, None, "certify", "certified",
                     rows_checked=rows_checked)
    finally:
        conn.close()

    return {"failures": failures, "warnings": warnings, "certified": certified}
