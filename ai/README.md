# AI City Summaries

One grounded narrative per `(city, date_utc)`, written into
**`marts.mart_city_summary`** and surfaced on the Power BI *AI City Summaries* page.

Each summary covers, in order: air quality → temperature → traffic → livability verdict
→ upcoming weather alerts. It is **grounded strictly in the marts** — the model gets the
day's numbers, the prior day's for delta framing, the hour each metric peaked, and any
active alerts, and is told never to invent a value. The 4 Macedonian cities are
weather-only, so they get an explicit "no traffic data" clause instead of fabricated
congestion.

Each row also carries a deterministic **`alert_level`** (`Severe`/`Warning`/`Normal`)
that drives the page's red/amber/neutral colour coding, and the paragraph's **length
scales with it** (~60/90/110 words). See *Alerts, severity and colour coding* below.

## Pipeline

```
fetch_inputs.py  →  generate_summaries.py  →  load_summaries.py
   (Postgres)          (Gemini API)             (Postgres upsert)
   no API call                                   no API call
```

Only the middle step calls a model. Both ends are deterministic SQL, so a bad
generation can always be re-run without touching the warehouse.

| File | Role |
|---|---|
| `common.py` | Shared DB connection + paths. Reads `POSTGRES_*` (host) **or** `SMART_CITY_PG_*` (Airflow container), so every script runs unchanged in both. |
| `fetch_inputs.py` | **Step A** — idempotent DDL for `mart_city_summary`, then builds the context pack (target day + prior day per city, with the star keys). |
| `summary_spec.md` | **The generation rules** — single source of truth, shared by both generation paths. Edit rules *here*. |
| `generate_summaries.py` | **Step B** — the Gemini call. Sends `summary_spec.md` as the system instruction + the pack as JSON. |
| `PROMPT.md` | **Step B, manual fallback** — the same step done in a Claude Code session (no API key). |
| `load_summaries.py` | **Step C** — upsert on `city_date_key`, re-deriving `city_key`/`date_key` from `mart_city_daily` so the star FKs are authoritative. |
| `_inputs/`, `_outputs/` | Gitignored scratch: context packs and generated summaries (the audit trail of what the model wrote). |

## Scheduled (the normal path)

The **`smart_city_ai_summary`** Airflow DAG (`@daily`) runs all three steps as
`fetch_pack → generate → load`, importing these modules directly.

- **It summarizes yesterday** — a complete UTC day whose `mart_city_daily` row is
  final. (`@daily` + `catchup=False`: the run's `ds` is the day the interval covers.)
- If the dev machine was off at midnight, the scheduler runs that interval when it
  next comes up, so a laptop-hosted Airflow doesn't silently skip the day.
- **No rows for that date → the run skips**, it does not fail. A day the ELT pipeline
  never ran is not an error worth an alert email.
- Backfill one day: *Trigger DAG w/ config* → `{"date": "2026-07-29"}`.

Deliberately **not** part of the hourly `smart_city_pipeline`: the summary is
daily-grain, so hourly runs would mean 24 model calls rewriting the same paragraph —
and mid-day runs would summarize a partial day.

## Manual run (host, venv313)

```bash
python ai/fetch_inputs.py                    # → ai/_inputs/<date>.json
python ai/generate_summaries.py              # → ai/_outputs/<date>.json  (Gemini)
python ai/load_summaries.py                  # → marts.mart_city_summary

# Useful flags
python ai/generate_summaries.py --dry-run    # build the pack, make no API call
python ai/generate_summaries.py --date 2026-07-29 --model gemini-2.5-flash
python ai/generate_summaries.py --from-file  # reuse an existing pack, don't re-query
```

**Fallback without an API key** — generate in a Claude Code session per
[`PROMPT.md`](PROMPT.md), then `python ai/load_summaries.py --model "claude-code"`.
The `model` column records which path wrote each row.

## Configuration

`GEMINI_API_KEY` in the **repo-root `.env`** (get one at
<https://aistudio.google.com/api-keys>) — one line covers both surfaces: the host CLI
loads that file directly, and the Airflow container receives it via
`env_file: ../.env`. Not `airflow/.env`, which compose uses only for `${VAR}`
interpolation. **The container reads env at start, so `docker compose up -d` is
needed after changing the key.**

Optional: `GEMINI_MODEL` (default `gemini-3.6-flash`), `GEMINI_THINKING_LEVEL`
(default `LOW`), `GEMINI_THINKING_BUDGET` (Gemini 2.x only, default `0`),
`GEMINI_MAX_OUTPUT_TOKENS`.

Cost: **one** API call per day for all cities (~5.7k prompt + ~2k output tokens) —
well inside the AI Studio free tier.

### Two model-API traps this hit during the build

- **`gemini-2.5-flash` is retired for new keys** — a key created in 2026 gets
  `404 … no longer available to new users`, *even though ListModels still lists it*.
  Don't trust the model list as an access list.
- **Thinking config is family-specific.** Gemini 3+ takes `thinkingLevel`
  (LOW/MEDIUM/HIGH); 1.x/2.x take a numeric `thinkingBudget`. Each family rejects the
  other's field with a bare `400 INVALID_ARGUMENT` that names no field.
  `_thinking_config()` picks by model name, and `generate()` retries once *without*
  thinking config if the API still rejects it — so a future rename costs a few
  thinking tokens instead of killing the daily run.

## Alerts, severity and colour coding

The pack carries two **separate** alert blocks per city, because they mean different
things:

- **`alerts.pollution_today`** — from `mart_pollution_alerts`, **measured** readings on
  the day being summarized. Stated as fact.
- **`alerts.upcoming_weather`** — from `mart_weather_alerts`, a **forecast** for days
  *after* it. Becomes a closing "looking ahead" clause, explicitly labelled a forecast.
  Collapsed to one row per (city, type, severity) — the raw table repeats a heatwave
  once per 3-hour forecast slot, which would flood the prompt with ~29 near-identical
  rows. `slots` counts forecast windows, **not** separate events.

**`alert_level` is computed by `classify_alerts()` in Python, not by the model.**
Power BI colours the summary page from it, so it must be reproducible and must agree
with the paragraph — if the model decided the colour, the colour would depend on its
wording. `generate_summaries._validate()` copies the level straight from the pack onto
each output row, so the model has no way to alter it.

| Level | Triggered by |
|---|---|
| **Severe** | Any alert row with `severity = 'Severe'` |
| **Warning** | Any `Warning` alert row; or `aqi_alert`; or `hours_poor_air > 0`; or `max_aqi ≥ 4`; or High/Severe congestion; or a Poor comfort index |
| **Normal** | None of the above |

`alert_headline` is the short reason behind the level ("Severe extreme heat",
"High PM2.5 (measured)"), and is **`"No alerts"` on a Normal day — never blank**. An
empty cell on the report reads as a load failure; explicit text reads as a finding.
A table visual cannot drop a column based on its data, so the column is always
present and this is what keeps it looking deliberate on a quiet day.

**Length scales with severity** — ~60 words Normal, ~90 Warning, ~110 Severe. A fixed
budget would force the model to drop content on exactly the days that matter, and the
first thing it drops is the alert. `_validate()` warns (never fails) past a soft
ceiling, to catch prompt drift.

## Intraday detail — and its hard limit

The pack includes the hour each metric peaked (`warmest_hour_utc`, `worst_aqi_hour_utc`,
`worst_congestion_hour_utc`) plus `hours_observed`.

⚠️ **Coverage is partial and this is not negotiable in the prose.** Airflow only runs
while the dev machine is on, so roughly 07:00–14:00 UTC is captured and **10 of 24 hours
have never been recorded** (hours 1–5 and 15–19: zero rows, ever). The spec therefore
requires peaks be framed as *"of the hours recorded"* and **forbids** any mention of
night, evening, overnight, rush hour or "24 hours" — a claim about those would be
fabrication, not analysis. `hours_observed` is passed in specifically to keep that
framing honest. This is also why there is no hourly *narrative* grain: a per-hour
summary would advertise a slicer that is 40%+ permanently blank.

## Design notes

- **Plain `requests`, not the google-genai SDK.** `requests` is already in venv313 and
  the Airflow image; adding an SDK there risks the dependency tug-of-war that forced
  dbt into its own venv. The REST endpoint is a stable single POST.
- **One call for all cities**, not one per city — cheaper and far under the per-minute
  rate limit. The trade-off is that a single response could drop a city, so
  `_validate()` checks that every input `city_date_key` comes back exactly once, with
  non-empty text and an unaltered key; anything else raises and Airflow retries.
- **Structured output** (`responseSchema`) so the reply is a parseable list, not prose.
- **`temperature: 0.2`** — grounded reporting, not creative writing.
- **`mart_city_summary` is not a dbt model.** It's written by `load_summaries.py`, so
  `dbt build` never drops it; its FKs point into the dbt-built star.
