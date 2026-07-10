# Migrating Surrogate Keys to `dbt_utils.generate_surrogate_key`

> **Change date:** 2026-07-10
> **Status:** ✅ implemented & verified live (`dbt build` = 85 nodes / 68 tests, 0 errors)
> **Commit:** `9b718a4` (model + package changes) — this guide is the how-to behind it.

## TL;DR

Every surrogate key in the project used to be hand-written as `md5(a || '|' || b)`. We switched
them all to the dbt-native **`dbt_utils.generate_surrogate_key([...])`** — same md5 underneath, but
NULL-safe, less boilerplate, and consistent. Because the util produces a **different key value**
than the old expression, the existing rows in the durable incremental `intermediate` tables were
**rewritten in place** (no history loss) with a one-off backfill macro. The marts, being full-rebuild
tables, regenerate on the next `dbt build`.

```
old:  md5(city || '|' || date_utc::text)
new:  {{ dbt_utils.generate_surrogate_key(['city', 'date_utc']) }}
```

If you are still on the old hand-written keys, this doc is the exact procedure to follow.

---

## Why migrate

| Benefit | Detail |
|---|---|
| **NULL-safe** | `md5(city \|\| '\|' \|\| incident_id)` returns **NULL** if any part is NULL (Postgres: `NULL \|\| x = NULL`). The util wraps every field in `coalesce(..., '_dbt_utils_surrogate_key_null_')`, so you always get a real key. |
| **Less boilerplate** | No manual `::text` casts, no `\|\|` glue, no separator to keep consistent. |
| **Consistent** | One recipe everywhere → the `relationships` FK tests stay valid because both sides of a join use the identical expression. |
| **Portable** | Uses `dbt.hash()` / `dbt.type_string()` — works across warehouses, not just Postgres. |

> ⚠️ The NULL benefit is partly *defense-in-depth* if you already filter NULLs (e.g.
> `where incident_id is not null`). The everyday win is consistency + less boilerplate.

---

## The one idea that makes it safe

**A surrogate key is not data — it's a deterministic function of columns already stored in the row.**

That means you can recompute a key **in place**, from the columns each row already has, without
going back to the original source. This is why the migration does **not** need `staging` (a ~1-day
buffer) and does **not** lose the years of history accumulated in the incremental `intermediate`
tables. Same columns + same formula = same key.

---

## What actually changes (old vs new)

Both end in `md5()`; only the string fed into it differs:

| | Old (hand-written) | New (`generate_surrogate_key`) |
|---|---|---|
| Separator | `\|` | `-` |
| NULL handling | whole key → NULL | `coalesce(..., '_dbt_utils_surrogate_key_null_')` |
| Cast | manual `::text` | automatic `cast(... as TEXT)` |

For Skopje at 06:00 the input string changes from `Skopje|2026-07-10 06:00:00` to
`Skopje-2026-07-10 06:00:00`, so the md5 output changes completely. **Every key value changes** —
that is expected, and why the backfill step exists.

Compiled form of `generate_surrogate_key(['city', "date_trunc('hour', observed_at)"])`:

```sql
md5(cast(
  coalesce(cast(city as TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
  coalesce(cast(date_trunc('hour', observed_at) as TEXT), '_dbt_utils_surrogate_key_null_')
as TEXT))
```

---

## The critical distinction: rebuilt tables vs incremental tables

| Layer | Materialization | Migration effort |
|---|---|---|
| **marts** (`dim_*`, `fct_*`, `mart_*`) | `table` — fully rebuilt every `dbt build` | **Just edit the SQL.** The next build regenerates all keys, self-consistent. |
| **intermediate** (`int_city_hourly_*`, `int_city_weather_forecast`) | `incremental` — only new rows are touched | **Edit the SQL *and* backfill the historic rows.** Incremental builds never revisit old rows, so old keys would keep the old format forever. |

> **Do NOT `--full-refresh` the incremental tables to fix them.** That rebuilds them from `staging`
> (a ~1-day buffer) and **discards all accumulated history**. Use the in-place backfill below.

---

## Migration procedure

### Step 0 — Install dbt_utils

Create [`dbt/smart_city/packages.yml`](../dbt/smart_city/packages.yml):

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.1", "<2.0.0"]
```

```bash
cd dbt/smart_city
dbt deps        # resolves & writes package-lock.yml (pins the exact version, e.g. 1.4.1)
```

**Commit `package-lock.yml`** — it is what pins the exact version for teammates/CI.

### Step 1 — Convert every `md5(...)` key in the models

Map each hand-written key to the equivalent util call (same columns, in order). The 15 call sites in
this project were:

| File | Key | New expression |
|---|---|---|
| `int_city_hourly_weather` / `_pollution` / `_traffic_flow` | `city_hour_key` | `generate_surrogate_key(['city', "date_trunc('hour', observed_at)"])` |
| `int_city_hourly_traffic_incidents` | `city_incident_key` | `generate_surrogate_key(['city', 'incident_id', 'observed_at'])` |
| `int_city_weather_forecast` | `forecast_key` | `generate_surrogate_key(['city', 'forecast_at', 'issued_at_utc'])` |
| `dim_city`, all `fct_*`, all `mart_*` (`md5(city)`) | `city_key` | `generate_surrogate_key(['city'])` |
| daily facts, `mart_city_daily`, `mart_temperature_trends` | `city_date_key` | `generate_surrogate_key(['city', 'date_utc'])` |
| `mart_forecast_latest` | `forecast_slot_key` | `generate_surrogate_key(['city', 'forecast_at'])` |
| `mart_weather_alerts` | `alert_key` | `generate_surrogate_key(['city', 'forecast_at', 'alert_type'])` |

**Consistency rule:** convert **all** models that share a key in one change. Table aliases don't
matter — `generate_surrogate_key(['w.city'])` == `generate_surrogate_key(['city'])` because the util
hashes the column *value*, not its name. This is what keeps the `relationships` tests green.

### Step 2 — Compile-check and confirm the generated SQL

```bash
dbt compile --select intermediate marts --target staging --profiles-dir C:/Users/Andrej/.dbt
# inspect the compiled key, e.g.:
grep -m1 "city_hour_key" target/compiled/smart_city/models/intermediate/int_city_hourly_weather.sql
```

Confirm you see the `md5(cast(coalesce(...) || '-' || coalesce(...) as TEXT))` shape.

### Step 3 — Back up the incremental tables (before any write)

```bash
# Row-count snapshot (compare after)
psql -h localhost -U postgres -d smart_city -c "
  select 'weather' t, count(*) from intermediate.int_city_hourly_weather union all
  select 'pollution', count(*) from intermediate.int_city_hourly_pollution union all
  select 'flow',      count(*) from intermediate.int_city_hourly_traffic_flow union all
  select 'incidents', count(*) from intermediate.int_city_hourly_traffic_incidents union all
  select 'forecast',  count(*) from intermediate.int_city_weather_forecast;"

# Portable dump of the whole schema
pg_dump -h localhost -U postgres -d smart_city -n intermediate -f intermediate_pre_surrogate.sql

# Instant in-DB rollback copies
psql -h localhost -U postgres -d smart_city -c "
  create table intermediate.int_city_hourly_weather_bak           as select * from intermediate.int_city_hourly_weather;
  create table intermediate.int_city_hourly_pollution_bak         as select * from intermediate.int_city_hourly_pollution;
  create table intermediate.int_city_hourly_traffic_flow_bak      as select * from intermediate.int_city_hourly_traffic_flow;
  create table intermediate.int_city_hourly_traffic_incidents_bak as select * from intermediate.int_city_hourly_traffic_incidents;
  create table intermediate.int_city_weather_forecast_bak         as select * from intermediate.int_city_weather_forecast;"
```

Reference counts from the 2026-07-10 run: weather **257**, pollution **814**, flow **546**,
incidents **245,140**, forecast **45,360**.

### Step 4 — Backfill the historic rows in place

The key insight (see [The one idea](#the-one-idea-that-makes-it-safe)) is realised by a **`run-operation`
macro** that runs `UPDATE <table> SET <key> = generate_surrogate_key([...])`. Using the util *inside*
the UPDATE means the rewritten historic keys are **byte-identical** to what fresh model builds emit —
no hand-typed SQL, no drift. The macro is
[`macros/backfill_surrogate_keys.sql`](../dbt/smart_city/macros/backfill_surrogate_keys.sql):

```bash
dbt run-operation backfill_surrogate_keys --target staging --profiles-dir C:/Users/Andrej/.dbt
```

> **Use the STORED column names.** `int_city_weather_forecast` stores `issued_at` (the alias of the
> model's `issued_at_utc`) — same value, different name — so the backfill lists `issued_at`. A plain
> `psql` UPDATE cannot call the util (it's a Jinja macro, not a SQL function); that's why the
> backfill runs *through dbt* via `run-operation`, which expands the macro.

Why not just `dbt build`? Incremental models only touch new rows (6h lookback), so a build would
**never** rewrite the old rows — hence the explicit backfill.

### Step 5 — Rebuild and test

```bash
dbt build --select intermediate marts --target staging --profiles-dir C:/Users/Andrej/.dbt
```

`dbt build` runs the models **and** their tests. The ones that prove the migration:
- **`unique` / `not_null`** on every key → still valid, one-of-a-kind IDs.
- **`relationships`** (`fct.city_key → dim_city.city_key`) → proves the new keys still join across
  the star. This is the real green light.

### Step 6 — Verify and clean up

```sql
-- row counts unchanged vs Step 3 (backfill must not add/drop rows)
-- keys are 32-char md5 hex with zero old-format leftovers:
select min(length(city_hour_key)), max(length(city_hour_key)),
       count(*) filter (where city_hour_key !~ '^[0-9a-f]{32}$') as old_format_leftover
from intermediate.int_city_hourly_weather;
```

Once green, drop the `_bak` tables (keep the `pg_dump` as the durable backup):

```sql
drop table intermediate.int_city_hourly_weather_bak,
           intermediate.int_city_hourly_pollution_bak,
           intermediate.int_city_hourly_traffic_flow_bak,
           intermediate.int_city_hourly_traffic_incidents_bak,
           intermediate.int_city_weather_forecast_bak;
```

---

## Automation: keep the pipeline self-sufficient

`dbt_utils` lives in `dbt_packages/`, which is **gitignored**, and this project **volume-mounts** the
dbt project into the Airflow container (`../dbt:/opt/airflow/dbt`). So:

- **Do not** bake `dbt deps` into the Dockerfile — the runtime mount shadows the image's copy.
- **Do** add a `dbt deps` task to the pipeline DAG, upstream of the dbt model tasks, so the pinned
  package is installed into the mounted `dbt_packages/` on every run (idempotent no-op when present).

Implemented in [`dag_smart_city_pipeline.py`](../airflow/dags/dag_smart_city_pipeline.py) as:
`wait_group >> dbt_deps >> dbt_staging >> dbt_intermediate >> dbt_marts`.

---

## Rollback

If anything looks wrong before you drop the `_bak` tables:

```sql
truncate intermediate.int_city_hourly_weather;
insert into intermediate.int_city_hourly_weather select * from intermediate.int_city_hourly_weather_bak;
-- …repeat per table…
```

or restore the whole schema from the dump:

```bash
psql -h localhost -U postgres -d smart_city -f intermediate_pre_surrogate.sql
```

Then `git checkout` the model files to revert the SQL edits.

---

## FAQ / gotchas

- **Will the keys match what they were before?** No — the value changes (different separator + NULL
  handling). There is no way to make the util reproduce the old `|`-delimited md5. That's fine as long
  as you convert every model sharing a key together, so both sides of each join agree.
- **Mixed key formats?** Only if you edit an incremental model but skip the backfill: old rows keep the
  old format, new rows get the new one. Harmless (the key is a within-table PK; nothing joins on it
  across tables) but ugly. The backfill normalises it.
- **`--target staging`, not `--target intermediate`?** `--target` selects a *connection profile*, not a
  schema. This project defines exactly one target (`staging`), and that single connection writes to all
  three schemas. `intermediate` is a schema/layer, not a target.
- **Do I need to touch the marts data?** No — they're `table` models; `dbt build` rebuilds them with
  the new keys automatically. Only the incremental `intermediate` tables need the in-place backfill.
