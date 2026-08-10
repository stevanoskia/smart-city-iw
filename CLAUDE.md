# Smart City Analytics Pipeline — Project Guide

## Project Purpose

End-to-end ELT data engineering platform that automatically ingests weather, air pollution,
and transportation data from public APIs and transforms it into analytical models with dbt.
Simulates a real-world smart city analytics solution.

The live pipeline runs entirely on PostgreSQL:
Airbyte → `staging` (raw JSON, Airbyte-written) → dbt `intermediate` (incremental hourly
facts + forecast history) → dbt `marts`, orchestrated hourly by Airflow, with a separate
`@daily` maintenance DAG pruning old raw rows. The `stg_*` JSON-parsing models are **ephemeral**
(compile inline into their consumers as CTEs — no DB object), so `staging` holds only raw JSON.

> **Marts layer:** ✅ **built** (2026-07-01) — star schema (dims + facts) + derived OBT
> + analytics marts, all green (`dbt build --select marts`, relationships/unique/
> accepted_values tests pass) and orchestrated as the `dbt_marts` step in the hourly DAG.
> `dim_city` is **derived from data — no seed**. Design/rationale live in
> `docs/marts_implementation_plan.md`; the build walkthrough in `docs/marts_build_guide.md`
> — both **local-only (gitignored)**, absent from a fresh clone.

---

## What Remains To Be Done

### Medium Priority (the marts now exist — these are unblocked)
| Task | Notes |
|---|---|
| BI dashboard | Power BI — **6 pages built + restyled to the example images** (2026-07-21): Executive Overview, Weather & Forecast, Air Quality (now incl. the pollution-alerts table — `mart_pollution_alerts` finally consumed), Weather + Pollution, Traffic & Congestion, **City Livability** (KPIs incl. `Best/Worst City`, livability ranking, comfort-vs-7d trend, temp/air/traffic composition stacked bars, heat-graded snapshot table) — model 15 tables / 26-rel star / **70 measures**, dropdown slicers synced across pages, Bing maps on Traffic + Weather+Pollution (Azure Maps needs org sign-in — unavailable on personal account). Remaining (optional): Sankeys via `.pbiviz` file import. ⚠️ Cyclic-refresh blocker **recurs** after structural changes/restarts — see RESET note + full-XMLA-refresh playbook in the Power BI section. |
| Noise / energy APIs | Additional smart city data sources |

### Bonus (not in original scope)
| Task | Notes |
|---|---|
| ✅ AI-generated city summaries | **Built 2026-07-29**, **automated on Gemini 2026-07-31** — now an unattended `@daily` Airflow DAG (`smart_city_ai_summary`). See Recently Completed + `ai/README.md`. |
| ✅ ML prediction pipelines | **Built + hardened 2026-08-05** — 6 models → `ml_predictions` schema, `@daily` `smart_city_ml` DAG. See Recently Completed + `ml/README.md`. |

### Recently Completed
- ✅ **`fct_traffic_incidents` — incident-grain fact; "Total Incidents" was 2.4× too high**
  (2026-08-05) — the Power BI card read **141K**; the real figure is **60,641**. Two stacked
  causes, both worth remembering:
  - **Summing a per-day distinct count gives incident-DAYS.** `Total Incidents` was
    `SUM(fct_traffic_daily[total_incidents])`, and that column is `count(distinct incident_id)`
    *per city-day*. 28.8% of incidents span >1 day (avg 1.57, max 6) → 1.5× overstatement.
    The dbt model was correct at its grain; nothing existed at *incident* grain to count.
  - ⚠️ **TomTom ROTATES `incident_id` roughly weekly.** The id embeds a `TTI-<uuid>` prefix
    that is a **batch identifier, not an incident identifier**: each uuid covers a distinct,
    non-overlapping date range (Jun 17–23, Jun 24–29, Jul 1–3, Jul 7–13 …) and **no id ever
    appears under two uuids**. The same physical roadworks is re-issued a new id every week —
    one Barcelona incident on Gran Via absorbed **111 different ids** over 24 days. So even
    `DISTINCTCOUNT(incident_id)` (96,872) over-counts. **Identity must come from location,
    never from TomTom's id.**
  - **Grain = `(city, road_from, road_to, feature_type)` + a session number.** Location alone
    would merge a June jam and an August jam on the same stretch, so it is sessionised.
  - ⚠️ **Sessions break on missed COLLECTION days, not calendar days.** Only **~58% of calendar
    days were collected** (Airflow runs while the laptop is on), so a wall-clock gap rule starts
    a new incident every time the *pipeline* was down — it swung the count 85K→42K on threshold
    alone. Each city gets a `dense_rank()` index of days it actually collected, and a session
    breaks only when the location is absent on a day we genuinely looked. Threshold is
    `var('incident_session_gap_days', 1)`.
  - `started_at` is carried but **not** used for identity or `date_key`: unstable (up to 20
    distinct values for one id) and 18.5% of incidents began before we first saw them (earliest
    2020). `date_key` = first observation, keeping rows inside the observation window.
  - **`materialized='table'`** on purpose — session boundaries are a window over each location's
    full history, so an incremental batch would compute them wrong at the boundary (same
    rationale as `mart_city_daily` / `mart_temperature_trends`).
  - **Power BI:** table imported (star **30 → 32**), `Total Incidents` **renamed to
    `Incident-days`** (kept — it is a valid disruption measure, just misnamed) and three new
    measures added: `Distinct Incidents`, `Major Incidents`, `Avg Incident Duration (days)`.
    The KPI card + the city×month heat matrix were repointed — including the matrix's
    **`FillRule` input and its selector `metadata`**, which a projection-only edit would have
    missed and silently broken the gradient.
  - **Verified:** `dbt build` green (model + 4 tests), 60,641 rows exactly matching the
    prototype, 0 orphaned FKs, rebuild byte-identical (checksum unchanged), and
    `SUM(days_observed)` = 132,695 ≤ the old 144,842 as the reconciliation predicted.
- ✅ **ML prediction pipelines — rebuilt on a leak-free, baseline-gated core** (2026-08-05) —
  the six forecasting ideas from the project brief now run as a scheduled `@daily` DAG
  (`smart_city_ml`) writing into a new **`ml_predictions`** schema. This reworks the first ML
  commit (PR #32), which could not run and whose reported accuracy was not measuring forecasting.
  Shipped guide: **`ml/README.md`** (committed, unlike `docs/`).
  - **Two blockers fixed.** (1) The `ml_predictions` schema **had no DDL anywhere in the repo** —
    all five `predict.py` scripts inserted into tables that did not exist, so none had ever run.
    Now **`ml/schema.sql`** (idempotent, applied automatically on train/predict). (2) The DB port
    defaulted to **5434**; this project runs Postgres on **5432**. It was wrong in five copies of
    the same `get_engine()` — now one `ml/common.py`, mirroring `ai/common.py`'s host-vs-container
    env handling. `requirements.txt` was also missing pandas/dotenv/psycopg2.
  - **Target leakage — the substantive fix.** Lags and targets were both built with
    `merge_asof(direction="nearest")`, which matches the closest row on **either side** of the
    wanted time. Measured on live data: **28%** of AQI rows and **26%** of temperature rows had
    `lag_1h` equal to the current value (a "lag" sourced from the present or future), and — worse —
    **48%** of traffic rows and **38%** of city-score rows had **`target` == the current value**,
    because a ±1h tolerance on a 1h horizon matches the row itself. Those models were being trained
    to return the number they were handed. `ml/featurelib.py` now uses `direction="backward"` for
    lags (never forward) and, for targets, returns the **matched timestamp** and discards any match
    closer than half the horizon.
  - **Evaluation was `train_test_split(random_state=42)` on a time series** — a random shuffle lets
    the model interpolate between hours it has already seen. Re-scored chronologically, it had
    understated error by **59%** (AQI) and **25%** (temperature). `ml/evaluate.py` now splits on a
    timestamp and scores every model against a **naive persistence baseline**; `train.py` **exits
    non-zero** when a model loses to it. Under the original leaky setup **3 of 4 regressors were
    worse than assuming no change** while printing good-looking numbers.
  - **Current honest skill** (snapshot 2026-08-05, ~7 weeks / 10 cities): `rain` AUC **0.964**
    vs 0.5 (+92.7%), `temperature` MAE **1.49 °C** vs 2.59 (+42.6%), `traffic` **+7.4%** — all beat
    persistence. **`aqi` (−27.6%) and `city_score` (−8.1%) genuinely do not**, and that is
    reported, not hidden: both already model the *delta* from the current value
    (`Pipeline.predict_delta`), and the remaining gap is a data-volume problem, not a tuning one.
    ⚠️ **These magnitudes move on every retrain** (the DAG refits daily on a fresh holdout) — read
    them from `ml_predictions.model_health`, never from a doc; `model_registry` keeps the history.
    *Which* models pass has been stable across runs.
  - **Rain probability (#4 in the brief) was missing entirely** — now built as an XGBoost
    classifier. Scored on **ROC AUC, not accuracy**, because rain is ~4% of observed hours (always
    predicting "dry" is 96% accurate and useless); training applies `scale_pos_weight`.
  - **Anomaly model reworked to be city-relative.** It fed raw pm2_5/pm10/aqi to IsolationForest,
    so it mostly learned *which cities are dirty* — a permanently polluted city looks anomalous
    every hour while a real spike in a clean city never stands out. Now robust z-scores against
    each city's own trailing 7-day median/IQR, **shifted one row** so a spike is not part of the
    baseline it is measured against. Also scores a trailing window rather than only the newest row
    per city (an anomaly log holding one row per city can't answer "when did Skopje spike?").
  - **Coverage constraint honoured.** With 10 of 24 hours permanently empty, lag lookups need
    tolerances — which is what made `direction="nearest"` so damaging. Rows with missing lags are
    now **kept**, since XGBoost learns a default branch for NaN; requiring every lag discarded ~75%
    of the data (fixing that alone took temperature from failing to **+44%** skill).
  - **Structure + orchestration.** Five near-identical `features/train/predict` triples collapsed
    into one shared core + six declarative `Pipeline` entries — that duplication is why one
    evaluation bug appeared in all five at once. The 3.4 MB of committed `.joblib` binaries are
    gone (build artifacts → `ml/_models/`, gitignored; the DAG retrains each run, which at ~1,700
    rows is cheaper than distributing artifacts). ML deps go in a **separate `ml_venv`** in the
    Airflow image and the DAG **shells out** to it — airflow 2.9.3 pins pandas/numpy, the same
    tug-of-war that forced dbt into its own venv. Needs `docker compose build` + the
    `../ml:/opt/airflow/ml` mount.
  - **Verified live end-to-end (2026-08-05):** all 6 pipelines train and score against the real
    warehouse; re-runs confirmed **idempotent** (upsert on natural keys, row counts unchanged).
    Also verified **in-container**: `airflow dags test smart_city_ml 2026-08-04` green in ~18s,
    all 3 tasks SUCCESS, with the two below-baseline models correctly *not* failing the DAG.
- ✅ **AI summaries — alerts, severity colour coding + intraday detail** (2026-07-31) — the
  summaries now carry **alerts** and an hour-level angle, and each row is classified for the
  Power BI page's colour coding. **Data side done + verified; the Power BI page edit is
  pending (needs Desktop CLOSED).**
  - **`mart_city_summary` gained `alert_level` + `alert_headline`** (idempotent `add column
    if not exists` in `fetch_inputs.DDL`). `alert_level` ∈ `Severe`/`Warning`/`Normal`,
    computed by **`classify_alerts()` in Python — never by the model**: the page's colour
    must be reproducible and must agree with the paragraph, and if the model decided it the
    colour would depend on its wording. `_validate()` copies it from the pack onto each
    output row, so the model cannot alter it. Triggers: `Severe` = any `severity='Severe'`
    alert row; `Warning` = any `Warning` row, or `aqi_alert`, or `hours_poor_air>0`, or
    `max_aqi>=4`, or High/Severe congestion, or Poor comfort.
  - **Two alert blocks, kept separate on purpose** — `mart_pollution_alerts` is **measured**
    (fact about the day); `mart_weather_alerts` is **forecast**, so only rows with
    `date_key > target` are included, as a closing "looking ahead" clause explicitly labelled
    a forecast. A past-dated forecast alert says nothing in a retrospective summary. The
    forecast rows are collapsed to one per (city, type, severity) — the raw table repeats a
    heatwave once per 3-hour slot (~29 near-identical rows), and `slots` is documented as
    forecast windows, **not** separate events.
  - **Length now scales with severity** — ~60 words Normal / ~90 Warning / ~110 Severe, and a
    `Severe` day **must open with the alert**. A fixed 60-word budget forced the model to drop
    content on exactly the days that matter. `_validate()` warns (never fails) past a soft
    ceiling, to catch prompt drift.
  - **Intraday peaks** (`warmest_hour_utc`, `worst_aqi_hour_utc`, `worst_congestion_hour_utc`
    + `hours_observed`) go into the pack, so the prose can say "air was worst at 13:00".
    ⚠️ **Bounded by the hourly-coverage constraint** (see that section): measured live —
    hours **1–5 and 16–19 have ZERO rows, ever** (weather also has none at 15); the bulk
    is 07–14 UTC. The spec therefore
    requires peaks be framed **"of the hours recorded"** and **forbids** night/evening/
    overnight/rush-hour/"24 hours" claims. `hours_observed` is passed in to enforce that.
    This is also why there is **no hourly narrative grain** — a per-hour summary would
    advertise a slicer that is 40%+ permanently blank.
  - **Verified live (2026-07-31):** all 3 loaded days reclassified (per day ~1 Severe /
    5–7 Warning / 2–4 Normal), 0 NULL levels, 0 orphaned FKs. Grounding hand-checked again on
    the new fields — peak hours, Skopje's measured 08:55 PM2.5 alert, Belgrade's Severe heat
    window (3 slots from 04 Aug 12:00), every delta — all exact. Word counts landed 64–91,
    inside every ceiling.
  - ✅ **Power BI colour coding authored (2026-07-31, PBI closed)** — checkpoint zipped first
    (`Documents/pbip_checkpoint_20260731_131653.zip`). Changes:
    - **TMDL** — `alert_level` + `alert_headline` added to `marts mart_city_summary` (both
      described); model now **81 measures** with 3 new ones on `mart_city_daily` (the single
      measure home): **`Summary Alert Color`** (`SELECTEDVALUE(alert_level)` → Severe
      `#E74C3C` / Warning `#F1C40F` / Normal `#E6EDF7` — the theme's own `bad` / `neutral` /
      `foreground` tokens, so it stays on-brand), plus `Severe Alert Days` /
      `Warning Alert Days` (defined, not yet on a visual — drag onto cards if wanted).
    - **PBIR** — page 8's `tableEx` (`e8000000000000000201`) gained an `alert_headline`
      column *between* `date_utc` and `summary_text` (the scannable flag reads before the
      paragraph explaining it), and per-column conditional formatting: `values[]` entries
      with `selector.metadata` = the column's queryRef and `fontColor` bound to the measure,
      on **all four** columns (`city` / `date_utc` / `alert_headline` / `summary_text`) — a
      table visual has **no "colour the whole row" switch**, so every column needs its own
      rule or the row reads half-flagged. The default (selector-less) entry keeps
      `fontSize`/`wordWrap`. Totals row switched off (meaningless on four text columns).
      ✅ **The tableEx totals property is `totals`, NOT `show`** — confirmed empirically:
      `objects.total` was written with *both* candidates, and on the next Desktop save PBI
      **dropped `show` and kept `totals`**, leaving
      `total: [{properties: {totals: {expr: {Literal: {Value: "false"}}}}}]`. (It is
      undocumented — theme JSON docs only cover `columnTotal`/`rowTotal` on matrices.)
      **Generalisable trick:** when a PBIR property name is unknown, write every plausible
      candidate, save in Desktop, then read the file back — PBI discards the unrecognised
      ones and normalises the survivor, so the file itself tells you the right name.
    - **Sorting** — a header click in Desktop persists as `query.sortDefinition`
      (`{sort: [{field: {Column: {...}}, direction: "Descending"}]}`). Page 8 sorts
      `date_utc` **Descending** so the newest day is always on top; within a day PBI falls
      back to city alphabetical, so no secondary sort key was needed.
    - ⚠️ **Deliberately NO `sortByColumn` for `alert_level`.** The obvious "sort Severe first"
      trick is a hidden `alert_sort = SWITCH(alert_level, ...)` calc column — which is
      *exactly* the circular dependency documented in the Page-7 note (a sort column whose
      formula reads the column it sorts) that made the whole PBIP fail to open. If severity
      ordering is ever wanted, add a **real integer column in Postgres**, never a calc column.
    - ⚠️ **PBIR conditional formatting needs a `dataViewWildcard` in the selector** — cost one
      round trip. A `values[]` entry with only `"selector": {"metadata": "<queryRef>"}` is
      **silently DISCARDED** on open: no error, no warning, the column renders in the default
      colour. The rule only applies with the `data` wildcard as well:
      `"selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}], "metadata": "<queryRef>"}`.
      Same failure class as the `syncGroup` incident (hand-written PBIR key in the wrong shape
      → dropped quietly). **Diagnostic that isolated it:** live DAX over XMLA proved the
      measure returned the right hex per city, so the model was exonerated and only the report
      binding was left; the working shape was then copied from the heat-graded `tableEx` on
      page 6 (`e6000000000000000304`). **Lesson: before hand-authoring any PBIR object, find an
      existing working instance in this same report and match its shape exactly.**
    - Note the 4 pre-existing measure-driven formats all use **`FillRule`** (gradients over
      numeric measures); the 3 new ones are the report's first **direct `Measure`** colour
      expressions (a measure returning a hex string = "Format by field value"). If that form
      ever proves not to render, the proven fallback is a numeric rank measure + `FillRule`
      with **explicit** min/mid/max values (auto-scaled endpoints would mis-colour a day where
      only two of the three levels are present).
    - Validated on disk: all report JSON parses, 339 lineageTags with **0 duplicates / 0
      malformed**, each new object present exactly once.
    - ⏳ **Needs one action in Desktop: open + Refresh** — the new columns exist in the model
      but hold no data until the `mart_city_summary` table is refreshed, so the colours read
      neutral until then. This is a **structural change**, so the first Desktop refresh may
      re-trip the autodetect cyclic-reference — run the full-XMLA-refresh playbook (Power BI
      section) if it does.
- ✅ **AI city summaries — automated on the Gemini API** (2026-07-31) — the summaries are now
  **scheduled and unattended**, closing the one trade-off of the 2026-07-29 build (it was a
  *triggered* Claude Code step, never a nightly cron). The generation step moved to the **Gemini
  API** (Google AI Studio key, free tier); the Claude Code path is **kept as a manual fallback**.
  Everything else — the context pack, the upsert, the grounding rules, `mart_city_summary`, the
  Power BI page — is unchanged, so the Power BI contract is untouched.
  - **`ai/generate_summaries.py`** (NEW, the only piece that calls a model) — one `generateContent`
    POST for **all** cities, with `responseSchema` structured output, `temperature 0.2`, and
    thinking off (`thinkingBudget: 0` — this is short grounded writing; thinking tokens were the
    bulk of the latency). Uses **plain `requests`, not the google-genai SDK**: `requests` is already
    in venv313 *and* the Airflow image, and adding an SDK to the Airflow env risks the same
    dependency tug-of-war that forced dbt into its own venv. Retries 429/5xx with backoff.
  - **One call for all 10 cities**, not one per city (cheaper, far under the per-minute free-tier
    limit). The risk that a single response silently drops a city is covered by `_validate()`:
    every input `city_date_key` must come back **exactly once**, unaltered, with non-empty text —
    otherwise it raises and Airflow retries.
  - **`ai/summary_spec.md`** (NEW) — the generation rules extracted into **one provider-agnostic
    file**, sent to Gemini verbatim as the system instruction *and* pointed at by `ai/PROMPT.md`.
    Both paths read the same spec, so the two can't drift. **Edit rules there, never in a caller.**
  - **`ai/common.py`** (NEW) — shared `get_conn()` that reads `POSTGRES_*` (host) **or**
    `SMART_CITY_PG_*` (container), so the same modules run in both; `fetch_inputs`/`load_summaries`
    were refactored into importable functions (`build_pack()` / `upsert()`) with their CLIs intact.
  - **DAG `smart_city_ai_summary`** (`@daily`, `airflow/dags/dag_smart_city_ai_summary.py`) —
    `fetch_pack → generate → load`, **importing** the modules (real tracebacks in the alert email,
    not an opaque exit code), pack + rows passed via XCom (~9.5 KB). Kept **out of** the hourly
    pipeline on purpose: daily grain, so hourly runs would be 24 calls rewriting one paragraph off
    partial-day data. **Summarizes yesterday** (`@daily` + `catchup=False` → `ds` = the complete UTC
    day the interval covers, whose `mart_city_daily` row is final); if the dev machine was off at
    midnight the scheduler runs that same interval when it next comes up, so a laptop-hosted Airflow
    doesn't skip days. **No marts rows for that date → the run SKIPS, not fails** (a day the ELT
    never ran isn't an error worth an alert). Backfill one day via *Trigger DAG w/ config*
    `{"date": "2026-07-29"}`. Same `alert_utils` failure/success emails as the other DAGs.
  - **Wiring:** `../ai:/opt/airflow/ai` mount + `GEMINI_MODEL` in `airflow/docker-compose.yml`
    (`GEMINI_API_KEY` arrives via the existing `env_file: ../.env`); Gemini vars documented in
    `.env.example`. ⚠️ The mount needs a **`docker compose up -d`** to take effect.
  - `mart_city_summary.model` records which path wrote each row (`gemini-3.6-flash` vs
    `claude-code`). Shipped guide: **`ai/README.md`** (committed, unlike `docs/`).
  - ⚠️ **Two Gemini API traps hit during the build** (both cost a 404/400 with a useless
    message — documented in `ai/README.md`): (1) **`gemini-2.5-flash` is retired for keys
    created after ~2026** — `404 "no longer available to new users"` **even though
    ListModels still lists it**, so the model list is not an access list; default is now
    the pinned **`gemini-3.6-flash`** (pinned over the `gemini-flash-latest` alias so a
    model swap is an explicit decision). (2) **Thinking config is model-family-specific** —
    Gemini 3+ takes `thinkingLevel` (LOW/MEDIUM/HIGH), 1.x/2.x take numeric
    `thinkingBudget`, and each family rejects the other's field with a bare
    `400 INVALID_ARGUMENT` naming no field. `_thinking_config()` picks by model name and
    `generate()` retries once *without* thinking config if it's still rejected, so a
    future rename degrades gracefully instead of killing the daily run.
  - **Verified live end-to-end (2026-07-31):** host CLI + the full DAG in-container
    (`airflow dags test … 2026-07-30`) green in ~9s, 10/10 upserted, **0 orphaned FKs**,
    one API call (5.8k prompt / 1.8k output tokens). **Grounding hand-checked against the
    context pack: every AQI, PM2.5 delta, peak temp, comfort score, incident count and
    label matched exactly — no invented values** — and all 4 MK cities emitted the
    "no traffic data" clause. Skip path verified (`2020-01-05` → task SKIPPED, no alert).
    DAG **unpaused**.
- ✅ **AI-generated city summaries — Claude Code / subscription path** (2026-07-29) — the bonus
  "AI-Generated Smart City Summary" feature, built to run on a **Claude subscription via Claude
  Code, NOT the Anthropic API** (no `ANTHROPIC_API_KEY`, no `anthropic` SDK, no per-token billing).
  The "model call" is a Claude Code session that reads the data and writes the narratives. Produces
  one ~60-word grounded paragraph per `(city, date_utc)` into a new **`marts.mart_city_summary`**
  table (air quality → temperature → traffic → livability verdict; weather-only MK cities get a
  "no traffic data" clause instead of invented congestion). Three pieces in the **`ai/` dir**
  (all committed except the scratch):
  - **`ai/fetch_inputs.py`** (deterministic, no API) — creates `mart_city_summary` (idempotent DDL),
    reads today + prior day from `mart_city_daily`, writes a context pack to `ai/_inputs/<date>.json`.
  - **`ai/PROMPT.md`** — the generation spec a Claude Code session follows, emitting
    `ai/_outputs/<date>.json` (a list of `{city_date_key, city, date_utc, summary_text}`).
  - **`ai/load_summaries.py`** (deterministic, no API) — upserts the outputs into `mart_city_summary`
    (`ON CONFLICT (city_date_key)`), **re-deriving `city_key`/`date_key` from `mart_city_daily`** via
    a join so the star FKs are authoritative (never trusts hand-copied keys).
  - `ai/_inputs/` + `ai/_outputs/` are **gitignored scratch**. Run order: `fetch_inputs.py` →
    generate in a Claude Code session (per `PROMPT.md`) → `load_summaries.py`. Verified live: 10
    cities loaded for 2026-07-29, 0 orphaned FKs, UTF-8 (`µg/m³`, `°C`, em-dash) intact.
  - **Power BI:** `mart_city_summary` **imported into the PBIP model** (TMDL table +
    `city_key → dim_city` / `date_key → dim_date` relationships — star 26 → **28**). ⚠️ First
    Desktop refresh after this structural add may re-trip the autodetect cyclic-reference — run the
    full-XMLA-refresh playbook (Power BI section). A dedicated **"AI City Summaries" report page**
    (id `b8000000000000000008`, 8th page) was authored via PBIR files: synced city slicer (`citySync`)
    + a `date_utc` dropdown slicer + a wide word-wrapped `tableEx` (city / date_utc / summary_text).
    Optional future polish: a `Latest Summary` measure pinned to `[Latest Date]` (model edit, live
    via XMLA) so a card always shows the newest day without picking a date.
  - ~~**Trade-off:** not a fully unattended nightly cron; it's a **triggered** step.~~ **Superseded
    2026-07-31** — the Gemini path above makes it a scheduled `@daily` DAG. This path survives as
    the **manual fallback** (no API key needed): generate per `ai/PROMPT.md`, then
    `python ai/load_summaries.py --model "claude-code"`. Full design/rationale in
    `docs/ai_city_summaries_plan.md` (local-only, gitignored).
- ✅ **Metadata-driven pipeline — config tables in Postgres** (2026-07-22) — pipeline
  configuration moved out of scattered YAML + hardcoded SQL into a **`config` schema** in the
  `smart_city` DB (the single source of truth), and the pipeline made a **generic, config-driven
  engine**. Adding a source/city/field is now an **INSERT**, not a code change. Pieces:
  - **`config` schema (8 tables)** — `config.sources` / `config.streams` / `config.locations` /
    `config.source_locations` (What/Where/When), `config.field_mappings` (the contract:
    `source_expr [::data_type] as target_column`, with `is_required` + `is_active` flags),
    `config.validation_rules` (quality thresholds; `severity` error/warn), `config.validation_runs`
    (validation audit log), `config.load_errors` (ingestion failures — see below).
    ⚠️ **The DB owns this schema — there is NO DDL in the repo** (changed 2026-08-10; was
    `config/schema.sql` + `migrations.sql` + `seed_config.py`). Tables are created/altered
    **directly in Postgres**; the only description of them is the table reference in
    **`metadata/README.md`** (this dir IS shipped/committed, unlike `docs/`), which must be updated
    in the same sitting as any `alter table`. The schema is **host state like `.env`/`pg_hba.conf`**
    — a rebuilt machine restores it from `Documents/smart_city_config_backups/` (`pg_dump
    --schema=config`), not from a clone. Old DDL recoverable via `git show 62e63a1:config/schema.sql`.
    SQL helper functions live in the DB (`\df config.*`): `config.add_city(city,lat,lon[,bbox])`
    (one call = locations + source_locations inserts), `config.set_city_active(city,bool)`,
    `config.remove_city(city)`.
  - **Config-driven staging (dbt "same engine")** — the 5 `stg_*.sql` are now one-liners
    `{{ build_staging('<stream>') }}`; the `build_staging` + `get_field_mappings` macros
    (`dbt/smart_city/macros/`) generate each SELECT from `config.field_mappings` at run time
    (`run_query`, guarded on `execute`). **Verified byte-identical** to the old hand-written output
    (gold-table EXCEPT compare on all 5 streams — cols + content diff (0,0)); full `dbt build` green
    (102 checks). So the intermediate/marts/Power BI contract is untouched. `raw_id`/`extracted_at`
    are emitted by the macro as a fixed header (Airbyte-managed, not in field_mappings).
  - **Data-contract validation gate** — new `validate_contract` DAG task (between syncs and dbt,
    `retries=0`) runs `config_utils.validate_streams`: Tier 1 stops the pipeline if a required field
    is missing/all-NULL; Tier 2 evaluates `validation_rules` (min/max/accepted_values/max_null_pct/
    min_row_count/freshness). Every check (pass+fail) is written to `config.validation_runs`
    **committed before the raise**, and a failure raises a multi-line `AirflowException` that the
    existing `on_failure` alert email renders — so you get an email listing exactly which
    field/threshold failed. Clean streams get a `certified` row. Also a cheap **config-sanity
    warning** (non-blocking, `status='config_warning'`): a `validation_rules` row whose
    `target_column` isn't a real field of the stream (a typo — it's free text, not an FK) would
    silently never fire, so it's surfaced as a warning instead. **Triage:** `validation_runs` has a
    `resolved` flag + a `config.open_validation_failures` view (unresolved failures, newest first) +
    `config.resolve_validation(run_id)` / `config.resolve_failures(stream)` helpers, so a handled
    failure can be marked without deleting audit history. Verified live (5 streams certified; a
    forced `min_row_count` breach failed only that stream + logged + un-certified it; a typo'd rule
    column warned without failing the run; resolve flow tested).
  - **Config-driven Airbyte setup + auto-detect** — `setup_airbyte.py` reads sources/streams/cities
    from `config.*` (was YAML); `main()` (host) manages the destination + LAN IP, `reconcile()`
    (container-safe) skips it. New `reconcile_airbyte` DAG task (first, best-effort — import inside
    the task, never raises) applies new config to Airbyte each run. Needs the `../ingestion/scripts`
    mount + `CONNECTION_IDS_FILE` env added to `docker-compose.yml`. ✅ **In-container reconcile
    VERIFIED (2026-07-23)** — `setup_airbyte.reconcile()` runs clean inside the scheduler container:
    reaches the config DB (`host.docker.internal`) + Airbyte, reuses the destination (skips LAN IP),
    updates both sources from `config.*`, writes `connection_ids.yml`. Host `setup_airbyte.py` path
    also verified (produces the same `connection_ids.yml`; DB config matches the old YAML exactly).
  - **YAML retired + DELETED (2026-08-10)** — `ingestion/config/sources.yml` + `connections.yml`
    survived only as the one-time seed input for `seed_config.py`; when that was deleted they lost
    their last reader and went with it. `ingestion/config/` now holds just the gitignored
    `connection_ids.yml` (+ a `.gitkeep` so the docker-compose mount target exists in a fresh
    clone). Edit `config.*` with SQL. See `metadata/README.md`.
  - **`config.load_errors` — ingestion failures are now queryable in SQL** (2026-08-10). A failed
    sync used to leave detail only in Airbyte's job log and a transient alert email, so *"which
    city failed to ingest last Tuesday?"* was unanswerable. `airbyte_utils.wait_for_sync` already
    parsed `attempts[].attempt.failureSummary.failures[]` to build that email — it now **also**
    persists it: one row per failure per run (`source_name`/`stream_name`/`city`/`job_id`/
    `failure_origin`/`failure_type`/`message`). Same triage idiom as `validation_runs` — a
    `resolved` flag, a `config.open_load_errors` view, and `config.resolve_load_error(id)` /
    `config.resolve_load_errors(source)`. **Logging never breaks ingestion**: the write is
    best-effort in its own connection+transaction and swallows any exception, so a config-DB
    hiccup can't convert a sync failure into a *different* failure and lose the real error.
- ✅ **Airbyte sync: trigger + wait merged into one task per connection** (2026-07-20) — the
  hourly DAG's `trigger_syncs` (push `job_id` to XCom) + `wait_syncs` (poll it) split was replaced
  by a single `syncs.sync_*` task per connection that triggers **and** waits. The split made
  retries useless: a failed sync only failed the *wait* task, whose retry re-polled the **same
  already-failed `job_id`** from XCom — a dead job never recovers, so the retry could never pick up
  a fix. This bit us on a network switch: after re-pointing the destination to the new LAN IP, the
  stuck run kept re-checking the dead job instead of re-triggering; recovery needed a manual clear
  of the trigger task. Merged, an Airflow retry re-triggers a *fresh* sync; `trigger_sync`'s 409
  handling still attaches to an already-running job so a retry won't double-trigger. Parallelism
  unchanged (the group still runs concurrently). DAG re-parses clean; tasks are now
  `syncs.sync_openweather_all` / `syncs.sync_tomtom_all` → `dbt_staging` → `dbt_intermediate` → `dbt_marts`.
- ✅ **`dbt deps` moved out of the hourly DAG → persistent named volume** (2026-07-20) — the per-run
  `dbt deps` task was removed from `smart_city_pipeline`. `dbt_utils` (1.4.1) now lives in a
  `dbt_packages` **Docker named volume** (declared in `airflow/docker-compose.yml`, layered over the
  `../dbt` bind mount at the `dbt_packages/` subpath), **populated once** and durable across container
  restarts/rebuilds. Rationale: `dbt_packages/` is gitignored + the project is bind-mounted, so it
  can't be baked into the image (the mount would shadow it); a durable volume is the stable
  alternative and takes a registry/network call off the hourly critical path. Trade-off vs. the old
  always-safe per-run step: the volume must be **populated once manually** (and re-populated after a
  `docker compose down -v` or a `packages.yml` change) — else the first model run fails "dbt_utils not
  found". One-time command + guidance in the README's *Start Airflow* section and the `dbt_marts` DAG
  notes below. The **host** `dbt deps` workflow (`~/.dbt`, venv313) is unchanged.
- ✅ **Marts facts → incremental `delete+insert`** (2026-07-20) — the 8 **append-only** marts
  models were switched from full table rebuild to `materialized='incremental'`, mirroring the
  intermediate layer: the **3 hourly facts** (`fct_weather_hourly`/`fct_pollution_hourly`/
  `fct_traffic_hourly`, key `city_hour_key`, 12h `observed_at` lookback), the **3 daily facts**
  (`fct_*_daily`, key `city_date_key`, 2-day `date_utc` source lookback — only today's row is
  mutable), `fct_forecast_accuracy` (key `forecast_key`, 2-day `forecast_at` lookback), and
  `mart_pollution_alerts` (key `alert_key`, measured/immutable history). The other **7 stay
  `table`** *on purpose* (headers say why): the 3 dims (tiny/static); `mart_city_daily` +
  `mart_temperature_trends` (rolling-window functions — a recent-rows batch would compute
  truncated averages at the boundary); `mart_forecast_latest` + `mart_weather_alerts`
  (forward-looking snapshots — passed slots must *disappear*, which `delete+insert` can't do).
  Verified byte-identical output three ways (full-refresh vs prior golden, incremental vs
  full-refresh, incremental run twice for idempotency) — schema **and** content md5 unchanged
  across all 15 tables, so the **Power BI (PBIP) column contract is untouched**; `dbt build`
  green (75 checks). No DAG change (the `dbt build --select marts` step just runs incrementally).
  ⚠️ First Desktop refresh after this may re-trip the autodetect cyclic-reference — run the
  full-XMLA-refresh playbook (Power BI section). PBIP checkpoint zipped before the change.
- ✅ **Surrogate keys → `dbt_utils.generate_surrogate_key`** (2026-07-10) — all keys across the
  intermediate + marts layers migrated from hand-written `md5(a || '|' || b)` to
  `dbt_utils.generate_surrogate_key([...])` (NULL-safe, `-` separator, consistent). `dbt_utils`
  added in `packages.yml`, pinned to **1.4.1** via `package-lock.yml` (installed into the persistent
  `dbt_packages` named volume — see the DAG-deps note below; originally a per-run `dbt deps` DAG step,
  removed 2026-07-20). Historic rows in the incremental `intermediate` tables were rewritten
  **in place** (no history loss) by `macros/backfill_surrogate_keys.sql`, run via
  `dbt run-operation`; `dbt build` green (85 tests incl. all `relationships` FK tests). That macro
  **stays** — it's **idempotent** (each key is a pure function of columns already in the row, so
  re-running converges on the same value) and nothing calls it automatically, so it's kept as the
  repair tool if keys ever drift from the models. Its migration *guide* was retired — the
  migration is done and the macro's own header documents it (recoverable from `9b718a4`).
- ✅ **Marts layer (star schema + OBT + analytics)** — **15** models in `models/marts/`: dims (`dim_city` *derived, no seed*; `dim_hour`; `dim_date`), daily facts (`fct_weather_daily`, `fct_pollution_daily`, `fct_traffic_daily`), hourly facts (`fct_weather_hourly`, `fct_pollution_hourly`, `fct_traffic_hourly`), `fct_forecast_accuracy`, the derived OBT `mart_city_daily`, and analytics marts (`mart_forecast_latest`, `mart_temperature_trends`, `mart_weather_alerts`, `mart_pollution_alerts`). Wired as the `dbt_marts` DAG step.
- ✅ **One Airbyte connection per API** — connectors are partition-routed (`ListPartitionRouter`) over a `locations` list, so a single connection (`openweather_all`, `tomtom_all`) ingests every city instead of one connection per city. Scales to many cities; Airflow + dbt unchanged.
- ✅ Expanded city coverage to **10 weather cities** (added Amsterdam, Belgrade, Brussels, Barcelona, Prilep, Bitola, Ohrid) and **6 traffic cities** (added Belgrade, Brussels, Barcelona); the 4 Macedonian cities are weather-only (no TomTom coverage)
- ✅ **Forecast** intermediate layer — incremental issue history (`int_city_weather_forecast`); the forward-looking *latest* (`mart_forecast_latest`) + prediction-vs-actual *accuracy* (`fct_forecast_accuracy`) models now live in the marts layer
- ✅ Incremental **hourly** intermediate layer (`int_city_hourly_*`) — preserves time-of-day + history; daily models roll up from it
- ✅ TomTom incidents `fields` fix — full incident detail now ingests (id, delay, magnitudeOfDelay, …)
- ✅ Split raw cleanup into a separate `@daily` `smart_city_maintenance` DAG
- ✅ Airflow XCom wait-task fix, on_failure_callback, per-task execution timeouts
- ✅ **Email alerts** — both DAGs email `ALERT_EMAIL` on failure (which task + error) and success
  (whole-pipeline / daily-cleanup done) via Gmail SMTP (`AIRFLOW__SMTP__*` env, App Password)

---

## Power BI Dashboard (in active build — 15 tables, clean 26-rel star, 79 measures, 7 pages)

> **Page 7 — Forecast & Accuracy (2026-07-22, violet accent `#A78BFA`, `b7…0007`):** image-2-style
> **day-tile strip** (matrix: columns = new `forecast_day` calc column "Wed 22"-style on
> `mart_forecast_latest`, sorted via `sortByColumn: forecast_date_utc`; values = `Forecast Icon URL`
> measure (dominant condition → OpenWeather PNG, ImageUrl) + Forecast Temp + Rain %) · 6 accuracy
> KPI cards · **city × day forecast heat matrix** · **MAE by lead-time** columns
> (`lead_time_bucket` given a hidden `lead_bucket_sort` calc column + `sortByColumn` — labels
> `<6h, 6-24h, 1-3d, 3-5d` sorted wrong otherwise; ⚠️ the sort column MUST derive from
> `lead_time_hours`, **never from the column it sorts** — `sortByColumn` + a formula reading the
> sorted column = a REAL circular dependency that makes the whole PBIP fail to open with "Unable to
> open file … circular dependency". Fixed on disk 2026-07-22; contrast the *bogus* autodetect
> cyclic-refresh error, which this is not) · predicted-vs-actual **scatterChart**
> (Category=city, X/Y = new `Avg Predicted/Actual Temp (C)` measures — scatterChart PBIR buckets
> Category/X/Y verified working) · hit rates + sample-size by city. ⚠️ The 3 calc columns
> (`forecast_day`, `lead_bucket_sort`) **materialize only after an in-Desktop Home → Refresh** —
> first open shows them blank. Forecast data: rolling ~6 days ahead (5-day/3-hour API), all 10
> cities, weekends included; live accuracy 2026-07-22: MAE 1.8 °C, within-2C 62%, rain hit 96%,
> condition hit 73% on 3,451 scored.

> **Full data dictionary + forecast measures (2026-07-22):** **100% descriptions** — all **76
> measures** and **227 data columns** carry a `Description` (the Model-view Properties / Fields-pane
> ⓘ tooltip), grounded in the dbt formulas (e.g. Comfort = 0.40 warmth + 0.40 clean-air + 0.20
> free-flow; `congestion_score = 1 − currentSpeed/freeFlowSpeed`; `hours_poor_air` = hours AQI≥4).
> Documents the `(period)` (slicer-aware) vs `Current */Latest *` (latest-date-pinned) distinction.
> Added via **live XMLA** (benign model objects — no close/reopen, just Ctrl+S). Power BI has **no
> native data-dictionary tab**, so a consolidated read is exported to
> `docs/powerbi_data_dictionary.md` (name + definition + DAX; regen script = session scratchpad
> `pbi_export_dictionary.ps1`, reads descriptions straight from the live model). ⚠️ Description
> writes need name-matching for °/µ/³ measure names — normalize (strip those chars) rather than
> embedding them in a PS5.1 script (mojibake); and `String.Replace(char,'')` is invalid — cast to
> `[string]` for the empty-replacement overload. The 6 forecast-accuracy measures are now
> **surfaced on Page 7** (see above); regenerate the dictionary export after adding measures. **`fct_forecast_accuracy` finally surfaced** with 6
> measures on `mart_city_daily`: `Forecast Temp MAE (C)` / `Temp Bias (C)` / `Temp Within 2C %` /
> `Rain Hit Rate %` / `Condition Hit Rate %` / `Forecasts Scored`. Live sanity (2026-07-22, 3,221
> scored): MAE 1.79 °C, bias +0.39, within-2C 62%, rain 97%, condition 75%. No Forecast-Accuracy
> *page* yet — measures are defined and ready to drag.

Live work on `C:\Users\Andrej\Documents\smart_city_dashboard.pbip` (Power BI **project**/PBIP,
connected to PostgreSQL `marts`, Import mode). It lives **outside** this git repo.
**Multi-page report plan: `docs/powerbi_dashboard_plan.md`** (gitignored). Build log:
`docs/powerbi_dashboard.md` (gitignored). Requirements/spec + example images:
`C:\Users\Andrej\Documents\smart-city-powerbi-skill\SKILL.md` and
`C:\Users\Andrej\Desktop\smart_city_examples\image*.png`.

### 🎨 Visual style = the example images (2026-07-21 restyle — in progress)
The user found the v2 pages too plain; each page is now restyled to **mirror a specific example
image** at `C:\Users\Andrej\Desktop\smart_city_examples\`. Those images are the **canonical style
reference** for layout, card rhythm, and colour — treat them as the spec when editing any page.
**One cohesive dark base** (`smart_city_theme.json`, unchanged bg `#0B1220`) + a **per-page accent**
applied at the *visual* level (hero fill, bar/gauge/donut colours) — never a per-page background.

| Page (file id) | Mirrors image | Accent | Hero panel fill |
|---|---|---|---|
| Executive Overview (`a9c1084738f01311493f`) | **image (2)** | teal `#22D3EE` | `#16324F` |
| Weather & Forecast (`b2000000000000000002`) | **image (4)** | warm orange `#F59E0B` | `#C2610C` |
| Air Quality (`b3000000000000000003`) | **image (5)** | AQI ramp green→gold→red | — |
| Weather + Pollution (`b4000000000000000004`) | **image (7)** | magenta `#EC4899` + cyan `#22D3EE` | `#173A6E` |
| Traffic & Congestion (`b5000000000000000005`) | **image (6)** *adapted* | teal `#22D3EE` + rose `#FB7185` | — |

**Hero pattern (2026-07-21 — the thing that finally made pages look like the images):** a hero is a
**layered composition**, never a single multiRowCard (that renders measure names as labels + a cramped
grid — the v3 "ugly" failure). Layers: `basicShape` rounded backdrop (fill above, `roundEdge` 18, title
on the shape) → `tableEx` weather-icon image → big `card` `Latest Temp Display` (~40pt, labels off) →
`card` `Latest Condition Display` (~15pt) → `card` `Latest Reading At` (10pt muted). All overlay cards
transparent (background/border/shadow/title off). P4's hero adds Wind/Humidity/Pressure mini-cards.

**Weather icon = OpenWeather PNG, not emoji (2026-07-21).** PBI's card visual cannot render emoji
glyphs (shows an empty box; `UNICHAR` measures verified correct over XMLA — it's purely a renderer
limit). The working approach: measure `Weather Icon URL` (condition → `openweathermap.org/img/wn/<code>@2x.png`,
`dataCategory: ImageUrl`) shown in a headerless **tableEx** (header can't truly hide — it's
**color-matched to the hero fill**; value fontSize drives the image row height). `Latest Condition Icon`
(emoji) still exists but is unused by visuals.

AQI ramp (matches what the `AQI Color` measure returns): `#2ECC71` `#A3D65C` `#F1C40F` `#E67E22`
`#E74C3C`. Theme data-colors reordered to lead teal→magenta→amber→green so multi-series pollutant
charts read on-brand. Cards softened to radius 18, fill `#131C33`.

**Images (2) and (4) are the same combined weather+AQI dashboard** (2 dark, 4 orange) — so there is
**no standalone "Weather+AQI" page**; image (4)'s look is folded onto the Weather page. Overview and
Weather share the hero + metric-grid + AQI-donut vocabulary, differentiated by **palette** + emphasis
(Overview = network/city-pills; Weather = 7-day forecast tiles + temp-anomaly).

**Data we don't have → substitutions (keep the image's card rhythm, real numbers):**
- Sunrise/Sunset card (img 2/4) → **"Data as of"** (`Latest Reading At`) + **Hi/Lo today** (`Max/Min Temp`).
- UV Index card (img 2/4) → **Cloudiness %** (`Current Cloudiness %`).
- Pedestrian/Car counters (img 7) → **Active Incidents** + **Avg Speed (km/h)** gradient cards.
- PM1 (img 7 donut) → **PM2.5 + PM10** only.
- AQI gauge stays **1–5** (never the images' 0–500), color-banded via `AQI Color`.

**Measures added for the restyle (2026-07-21 → model now 61):** via XMLA — `Latest Condition Icon`
(emoji, superseded by the icon-URL approach above), `Visibility (km)` (per-city latest
`visibility_m`/1000 — reads a flat ~10.0, low variance), `Prime Pollutant` (worst pollutant, each
species normalized by its OpenWeather index-4 onset: PM2.5 50 / PM10 100 / NO2 150 / O3 140 /
SO2 250 / CO 12400 µg/m³ — currently O3 everywhere, correct for clean summer air). Via TMDL (PBI
closed): `Weather Icon URL`, `Latest Temp Display` ("23.6 °C"), `Latest Condition Display`
(capitalized), and **6 period traffic measures** for Page 5 — `Congestion (period)`,
`Avg Speed (period)`, `Free-Flow Speed (period)`, `Avg Delay (period)`, `Total Incidents`,
`Total Closures` (plain aggregations that **respond to slicers**, unlike the date-pinned `Current *`
family which does `ALL('marts dim_date')`; that's why Page 5's charts/KPIs use them).

**Page 5 — Traffic & Congestion (image 6, honestly adapted, 2026-07-21):** image 6's street-level
jam segments are **not reproducible** (we hold city points, no road geometry) → Azure Map with city
bubbles instead; its day-part peak bars are **not viable** (06–15 UTC only). Built: 6 period-measure
KPI cards, City + Month **dropdown** slicers, congestion-by-city bar, speed-vs-free-flow 2-measure
bar, incidents **city × month heat matrix** (needed `sortByColumn: month` on
`dim_date[month_name]` — added, else months sort alphabetically), incidents-by-city columns; map gap
reserved top-right (x504,y136,752×312). Macedonian cities disappear from traffic charts naturally
(LEFT-join → BLANK measures), so no page filter is needed; the slicer still lists all 10.

**Interactivity (2026-07-21):** all city slicers are **Dropdown** mode and carry
`syncGroup: citySync` (P3+P5's Month slicers: `monthSync`) — a city picked on one page follows to
all pages. ⚠️ `syncGroup` is **NOT a root-level key** in the PBIR visualContainer schema (PBI
stripped it on open with "additional property" warnings); it lives **inside the `visual` object**,
and the working setup was done via **View → Sync slicers → Advanced options → group name** (PBI
then wrote the correct JSON itself). P4 gained its first city slicer. Cross-highlighting is on
everywhere (`drillFilterOtherVisuals: true`). ⚠️ **Theme needs a `slicer` section** — without
`items`/`dropdown` styles the dropdown popup is PBI's default white with the theme's near-white
text = unreadable; fixed in `smart_city_theme.json`. Dropdown slicers need **~90px height** (title
+ header + input box) — at 56px the input clips off and the slicer looks dead/unselectable.

**Maps = classic Bing "Map" visual, NOT Azure Maps (2026-07-21).** The Azure map visual now
requires a **work/school Microsoft sign-in** — personal Gmail is rejected outright, so on this
machine Azure Maps is unusable. Substitute: the classic **Map** visual (needs File → Options →
Security → "Use Map and Filled Map visuals" ticked; no sign-in). Wells: `dim_city[latitude]`/
`[longitude]`, `city` → Legend, Size = `Congestion (period)` (Traffic) / `Current AQI (1-5)`
(Weather+Pollution). Map styles → Dark theme. Same constraint applies to **AppSource custom
visuals** (Sankey): in-app import needs sign-in; a `.pbiviz` **imported from file** (e.g.
Microsoft's powerbi-visuals-sankey GitHub releases) works without.

### How Claude edits Power BI (two surfaces — keep PBIP, not PBIX)
- **PBIP is required** for the file-authoring half: the project is text — **TMDL** (model) + **PBIR**
  (report JSON) — so Claude can read/edit/diff it. A binary `.pbix` cannot be edited this way (only
  the live-model half below would work). Convert via *File → Save as → Power BI project* if ever on
  `.pbix`.
- **Model edits — LIVE, no reopen.** While PBI Desktop is open it hosts an Analysis Services engine
  (`msmdsrv`) on a local port. Claude connects over XMLA using the GAC-installed **ADOMD.NET + TOM**
  assemblies (no install needed) to read (DAX/DMV, e.g. `$SYSTEM.DISCOVER_CALC_DEPENDENCY`) and write
  measures / calc columns (TMSL/TOM). Helper scripts (session scratchpad): `pbi_query.ps1` (auto-finds
  port+catalog, runs DAX/DMV), `pbi_add_measures*.ps1`, `pbi_add_calccol.ps1`, `pbi_list_rels.ps1`.
  Port changes each launch — always auto-discover via `Get-Process msmdsrv`.
  ⚠️ **Calc columns added via TOM stay empty until the user does an in-Desktop Home → Refresh**
  (external `refresh type=calculate` does not materialize them); measures work immediately.
- **Report/canvas edits — files, PBI CLOSED.** Visuals/pages are authored by writing PBIR
  `visual.json` / `page.json` files (register pages in `pages/pages.json`), then the user reopens.
  PBI **owns the files while open**, so this half and the user's UI edits are mutually exclusive in
  time — alternate (save+close → Claude edits → reopen). Azure Maps, gauges, and Sankey custom
  visuals are added via the **UI** (not hand-authored).

### Status
### ⚠️ Four Power BI settings that cause "A cyclic reference was encountered"
All live in **File → Options → Current File → Data Load** (make sure it's the **CURRENT FILE**
scope, not GLOBAL), are **per-file** (not in git — they do **not** survive rebuilding the PBIP from
scratch, **nor a device restart / auto-recovery / external TMDL edit** — see the 2026-07-20 note),
and produce the *same* misleading error. If a refresh fails with "cyclic reference", check these
**first** — the model is almost always fine.

| Setting | Group | Must be | Why |
|---|---|---|---|
| **Auto date/time** | Time intelligence | ☐ **off** | Generated a `DateTableTemplate_*` + ~13 hidden `LocalDateTable_*` tables whose date-variation relationships formed a cycle. Fixed 2026-07-13. Use `dim_date` instead. |
| **Autodetect new relationships after data is loaded** | Relationships | ☐ **off** | Matches shared key columns across facts on *load* → junk fact-to-fact links. Fixed 2026-07-14. |
| **Update or delete relationships when refreshing data** | Relationships | ☐ **off** | Same mechanism but fires on **refresh** (greys out once the two below/above are off). Untick all three in the group together. |
| **Import relationships from data sources on first load** | Relationships | ☐ off | Same mechanism, fires on a fresh open. Relationships are defined explicitly in `relationships.tmdl`, so nothing is lost. |

**The autodetect trap (2026-07-14).** All three hourly facts share a **`city_hour_key`** column (plus
`city`, `date_utc`, `observed_at`); the daily facts + OBT + alert marts share `city_key`/`date_key`/
`city`. After a refresh, the relationship-autodetect pass matched those columns and wired the fact
tables **to each other**, closing a loop against `dim_city`/`dim_date` → genuine cycle → **every**
query blocked (an arbitrary set of tables named each time — even innocent dims like `dim_hour`, since
it's a *global* cycle-detection failure, not per-table). It never showed up over **XMLA** (external
refresh doesn't run Desktop's autodetect) — which is what proves the model itself is sound. The
refresh fails *at* the autodetect step and rolls back, so the junk relationships never persist; the
star always reads clean.

**The settings RESET — expect recurrence (2026-07-20).** These relationship boxes were confirmed
**off** yet a Desktop refresh still cyclic-failed. Diagnosis (all verified over XMLA): model
structurally clean (26 fact→dim rels, no calc tables, no column variations, no bidirectional
filters, no shared M query, only 2 trivial same-table calc columns); a **full-model XMLA refresh of
all 15 tables succeeded**; only Desktop's refresh path failed. Root cause: the **first** Desktop
refresh after a *structural change* (importing `mart_pollution_alerts`, which added fresh
`city_key`/`date_key`/`city` match surface) tripped the autodetect pass once. A full XMLA refresh
(brings every table to a consistent `Ready` state) followed by a repeat Desktop refresh cleared it,
and it stayed green. **Playbook when this recurs:** (1) don't trust that the boxes "look off" — the
model is the thing to check; (2) run a **full-model XMLA refresh** (`RequestRefresh(Full)` +
`SaveChanges()` over TOM — see the session scratchpad `pbi_refresh_full.ps1`); (3) then refresh in
Desktop once more. The star holds at **26 relationships, all fact→dim**.

- ✅ **Cyclic-reference refresh blocker FIXED** — root cause was **Auto Date/Time** (see table above).
  All KPIs green.
- ⚠️ **Refresh cyclic-reference — recurs after structural changes** (root cause **Autodetect new
  relationships**, first fixed 2026-07-14; re-appeared + re-cleared 2026-07-20 — see the settings
  section above for the RESET note + full-XMLA-refresh playbook). Refresh green; star holds at **26**
  fact→dim relationships.
- ✅ **Filters pane readability FIXED (2026-07-14)** — the theme set a dark page background but defined
  no `outspacePane`/`filterCard` styles, so the Filters pane kept Power BI's default **light-theme
  black text** → black-on-black, unreadable. Added both (incl. the `Applied`/`Available` card states)
  to `smart_city_theme.json`. ⚠️ **Editing the theme file does nothing on its own** — it must be
  re-imported via **View → Themes → Browse for themes**; Power BI bakes a copy into
  `Report/StaticResources/RegisteredResources/`.
- ✅ **Model layer complete** — **15** marts tables loaded (all of `models/marts/`; `mart_pollution_alerts`
  imported 2026-07-15 — see below); clean star (**26** relationships, all fact→dim, no junk fact-to-fact
  links); **49 measures** + 2 calc columns (`AQI Category (daily)` on `fct_pollution_daily`,
  `Congestion Band` on `fct_traffic_hourly` — both **bare-ref**, never self-qualified) added live.
  All 49 measures live on `mart_city_daily` (single measure home) even when they aggregate other
  tables' columns. Measure families: `[Latest Date]` anchor + 25 date-pinned `Current *`; 7
  point-in-time `Latest *` (read the *hourly* facts, `AVERAGEX` over `dim_city[city_key]`); 9 plain
  aggregations; 2 label/colour SWITCHes (`AQI Color` defined but not yet wired to a visual).
- ✅ **`Current *` date-filter pattern fixed (2026-07-15)** — the 29 date-pinned measures were
  rewritten from `FILTER(ALL(<fact>[date_utc]), <fact>[date_utc] = d)` to
  `<fact>[date_utc] = d, ALL('marts dim_date')`. The old form cleared the fact's *own* date column
  but **not** the filter arriving through `dim_date → fact` on `date_key`, so any future date slicer
  would intersect to empty and blank every `Current *` card. Both forms return identical values with
  no date slicer (verified live, side by side), so **no existing visual changed** — the fix only
  removes the latent trap. `[Rain Probability %]` was left alone (reads `mart_forecast_latest`,
  which has no `dim_date` link).
- ✅ **`mart_pollution_alerts` imported (2026-07-15)** — the 15th marts model, previously built in dbt
  but never imported. Now an Import-mode table with `city_key → dim_city` + `date_key → dim_date`
  relationships (the two that took the star 24 → 26). 14 rows, verified live. Air-quality analogue of
  `mart_weather_alerts`, but built from **real hourly readings** (`fct_pollution_hourly`), not a
  forecast. ⚠️ **No visual consumes it yet** — surfacing it on Page 3 (an alerts table mirroring
  Page 1's weather alerts + an `Active Pollution Alerts` measure) is a *report* edit, PBI **closed**.
- ✅ **Page 1 (Executive Overview)** — **done** (earlier docs said it was still the v1 cramped grid;
  that's stale). Renamed to "Executive Overview", on the v2 standard (KPI cards 190×96 from x=24,
  short custom titles, category labels hidden), with the point-in-time `Latest *` "Live Reading"
  multi-row card, temp-trend line (Avg Temp + Temp 7d Avg, **no legend**), and weather-alerts table.
  Still missing only the **Azure Map** in the reserved centre gap.
- ✅ **Page 2 (Weather & Forecast)** — 6 condition cards, temp trend + 7-day-avg line, 7-day forecast
  columns, chance-of-rain bars, temp-anomaly-by-city, city slicer.
- ✅ **Page 3 (Air Quality)** — AQI gauge, 6 pollutant cards, Avg-AQI-by-city bar, AQI-category
  donut, AQI heatmap-calendar matrix (mirrors example image (8)), city slicer.
- Dark theme (`smart_city_theme.json`) applied.

### ⚠️ Hourly coverage constraint — no diurnal / peak-hour analysis (found 2026-07-14)
The hourly facts only cover **06:00–15:00 UTC** — Airflow runs only while the dev machine is on, so
there is **no evening or overnight data at all**:

| Table | Distinct hours | Bulk of rows |
|---|---|---|
| `fct_weather_hourly` | 14 / 24 | 07h–14h |
| `fct_pollution_hourly` | 15 / 24 | 07h–14h |
| `fct_traffic_hourly` | 15 / 24 | 07h–14h |

**Re-measured 2026-08-05.** Hours **1–5 and 16–19 have ZERO rows, ever** — 9 of 24 —
and `int_city_hourly_weather` additionally has none at 15, so **9–10 hours are permanently
empty depending on the stream** (an earlier note said a flat "1–5 and 15–19 / 10 hours";
hour 15 does carry rows for pollution and traffic). Hours 0 and 20–23 hold only 4–10 rows
each across all history — one or two stray late nights, not coverage. Per-day capture is
3–12 distinct hours, typically 6–9. The conclusion is unchanged, which is why the AI summaries frame every
intraday peak as *"of the hours recorded"* and are forbidden from mentioning
night/evening/overnight/rush-hour, and why there is no hourly *narrative* grain.

**Consequence:** peak-hour / time-of-day analysis is **not viable** and must not be shipped — a
`day_part` chart would render Morning+Afternoon only, with Night/Evening empty, which reads as a
finding ("no traffic at night!") when it is really a sampling artifact. This **cancels** the planned
Page-4 peak-hour column and **Sankey #3** (`Day Part → Congestion Band`). It predates the new marts
(`fct_traffic_hourly` always had it). Revisit only if the pipeline ever runs 24/7 (cloud/always-on host).

**The hourly facts' honest use is point-in-time "latest reading" semantics**, not diurnal curves —
i.e. the real newest observation (the hero card in example images (2)/(4)), replacing "current" KPIs
that are really daily averages of `mart_city_daily`.

### Layout & readability standard (v1 pages came out cramped — fix 2026-07-13)
Full spec in `docs/powerbi_dashboard_plan.md`. Essentials: **≤ 6 KPI cards + ≤ 5 other visuals per
page** (split the page if more). 1280×720, **24 px outer margin**, **16 px gutter**, snap to grid.
KPI cards **190×96** with a **short custom `title`** + **hidden category label** (long measure names
like `Current PM2.5 (µg/m³)` clip otherwise — keep units in the measure, short name on the card).
Charts **≥ 460×280**. **Line charts: never a Legend + multiple value measures together** (Power BI
error *"too many columns in the Legend bucket"* — that broke the v1 Page-2 trend line; fix = two
measures `Avg Temp (°C)` + `Temp 7d Avg (°C)` with **no** legend). One city slicer per page (sync later).

### To be implemented (per `docs/powerbi_dashboard_plan.md`)
- **Page 1** — ✅ rebuilt (Executive Overview, v2 layout, `Latest *` Live Reading card — see status
  above). Remaining: only the **Azure Map** (UI) in the reserved centre gap.
- **Page 3 pollution alerts** — surface the newly-imported `mart_pollution_alerts` as an alerts table
  (mirror Page 1's weather-alerts table) + an `Active Pollution Alerts` measure. Report edit, PBI closed.
- **Page 4 Traffic & Congestion** — congestion/speed/incident cards, congestion-by-city bar,
  speed-vs-free-flow, congestion-over-time **by date** (⚠️ *not* peak-hour by `day_part` — see the
  hourly coverage constraint above), jam map (UI).
- **Page 5 City Livability** — livability ranking, comfort index/trend, component breakdown; add the
  `Best/Worst City` text measures. No data constraints on this page.
- **Sankeys** (custom visual, UI): City→AQI Category, City→Congestion Label.
  (~~Day Part→Congestion Band~~ — cancelled, no evening/overnight data.)
- **Deferred**: weather-type donut (needs a row-count measure, add live), cross-page **slicer sync**
  (`View → Sync slicers`), styling/label polish.

### Example images — what our data can and cannot mirror
Images at `C:\Users\Andrej\Desktop\smart_city_examples\image*.png` are a **visual vocabulary only** —
the numbers/domains are not ours. Reproducible: dark card grid + hero "last updated" card (2)(4);
pollutant dot-cards + AQI gauge (3)(4)(5); AQI-by-city bar + category donut (5); heatmap calendar (8);
7-day forecast tiles + chance-of-rain bars (1)(3)(4); map bubbles (5)(7)(8) via Azure Maps.
**Not reproducible — do not chase:** sunrise/sunset (1)(2)(4) and UV index (2)(4) are *not ingested*;
the 0–500 AQI gauge (3)(4)(5) must stay **1–5** (OpenWeather scale); image (6)'s per-street jam
segments need road geometry we don't have (we hold 6 city *points*, not segments); image (7)'s
pedestrian/car counters and image (0)'s energy/parking are different IoT domains entirely.
**Now newly available** via `fct_weather_hourly`: `visibility_m`, `wind_gust_ms`, `weather_description`
— so the "Visibility" card from (2)/(4) *is* possible (earlier docs said it wasn't). Caveat:
`visibility_m` reads a flat 10000 (OpenWeather's clear-sky cap) in every row sampled — check its
variance before spending a card on it.

## Current Status (as of 2026-07-09)

### Infrastructure
| Component | Status | Notes |
|---|---|---|
| PostgreSQL 18 | ✅ Running | localhost:5432, DB: smart_city — ingestion/landing DB |
| Airbyte (abctl) | ✅ Running | localhost:8000, Kind/Kubernetes |
| Airbyte destination | ✅ Configured | smart_city_postgres → staging schema (raw JSON) |
| Airflow | ✅ Running | localhost:8080, DAG smart_city_pipeline deployed |

### Data Ingestion (APIs)
| API / Stream | Status | Cities | Notes |
|---|---|---|---|
| OpenWeather current weather | ✅ Working | Skopje, Berlin, London, Amsterdam, Belgrade, Brussels, Barcelona, Prilep, Bitola, Ohrid (10) | hourly sync |
| OpenWeather air pollution | ✅ Working | Skopje, Berlin, London, Amsterdam, Belgrade, Brussels, Barcelona, Prilep, Bitola, Ohrid (10) | hourly sync |
| OpenWeather 5-day forecast | ✅ Working | Skopje, Berlin, London, Amsterdam, Belgrade, Brussels, Barcelona, Prilep, Bitola, Ohrid (10) | hourly sync |
| TomTom traffic flow | ✅ Working | London, Berlin, Amsterdam, Belgrade, Brussels, Barcelona (6) | hourly sync |
| TomTom traffic incidents | ✅ Working | London, Berlin, Amsterdam, Belgrade, Brussels, Barcelona (6) | hourly sync; full detail via `fields` param |

> **10 weather cities, 6 traffic cities.** Traffic covers London, Berlin, Amsterdam, Belgrade,
> Brussels, Barcelona; the 4 Macedonian cities (Skopje, Prilep, Bitola, Ohrid) are weather/pollution
> only — TomTom has no segment/incident coverage there. Add a city with
> `select config.add_city('Zagreb', lat, lon [, bbox])` and re-run `setup_airbyte.py`.

### dbt Transformation
| Layer | DB | Model | Status |
|---|---|---|---|
| Staging | PostgreSQL | `stg_current_weather` | ✅ Built |
| Staging | PostgreSQL | `stg_air_pollution` | ✅ Built |
| Staging | PostgreSQL | `stg_weather_forecast` | ✅ Built |
| Staging | PostgreSQL | `stg_traffic_flow` | ✅ Built |
| Staging | PostgreSQL | `stg_traffic_incidents` | ✅ Built |
| Intermediate (hourly facts) | PostgreSQL | `int_city_hourly_weather` | ✅ Built (incremental) |
| Intermediate (hourly facts) | PostgreSQL | `int_city_hourly_pollution` | ✅ Built (incremental) |
| Intermediate (hourly facts) | PostgreSQL | `int_city_hourly_traffic_flow` | ✅ Built (incremental) |
| Intermediate (hourly facts) | PostgreSQL | `int_city_hourly_traffic_incidents` | ✅ Built (incremental) |
| Intermediate (forecast) | PostgreSQL | `int_city_weather_forecast` | ✅ Built (incremental issue history) |
| Marts (dims) | PostgreSQL | `dim_city` (derived), `dim_hour`, `dim_date` | ✅ Built |
| Marts (daily facts) | PostgreSQL | `fct_weather_daily`, `fct_pollution_daily`, `fct_traffic_daily` | ✅ Built |
| Marts (extra facts) | PostgreSQL | `fct_traffic_hourly`, `fct_weather_hourly`, `fct_pollution_hourly`, `fct_forecast_accuracy` | ✅ Built |
| Marts (OBT + analytics) | PostgreSQL | `mart_city_daily`, `mart_forecast_latest`, `mart_temperature_trends`, `mart_weather_alerts`, `mart_pollution_alerts` | ✅ Built |

### Orchestration
| Component | Status | Notes |
|---|---|---|
| Airflow DAG `smart_city_pipeline` | ✅ Deployed | Triggers all syncs → dbt staging → dbt intermediate → **dbt marts** (all build+test). |
| Airflow DAG `smart_city_maintenance` | ✅ Deployed | `@daily` — prunes old `staging` (raw JSON) rows per retention policy |
| Hourly schedule | ✅ Configured | `@hourly` via Airflow scheduler |
| Airbyte OAuth auth | ✅ Working | client_id/client_secret via Applications API |

---

## Architecture

```
                        ┌──────────────────────────────────────┐
                        │          Apache Airflow               │
                        │   smart_city_pipeline DAG (@hourly)  │
                        └──────┬───────────────┬───────────────┘
                               │ triggers sync  │ triggers dbt
                               ▼               ▼
┌──────────────────┐    ┌───────────┐    ┌────────────────────────┐
│ OpenWeather API  │    │           │    │  PostgreSQL 18         │
│ TomTom API       │───►│  Airbyte  │───►│  staging (raw) ◄── dbt* │
└──────────────────┘    │           │    │  intermediate  ◄── dbt │
                        └───────────┘    │  marts         ◄── dbt │
                             :8000       │  (*stg_* ephemeral)    │
                                         └────────────────────────┘
```

**Single-database ELT (current):** everything lives in one PostgreSQL database across three schemas.
- **`staging`** — Airbyte writes raw, append-only API-snapshot JSON here (short buffer). The
  `stg_*` dbt models parse this JSON but are **ephemeral** — they compile inline into `int_*`/
  `dim_city` as CTEs and create no DB object, so `staging` contains only the raw Airbyte tables.
- **`intermediate`** — durable dbt building blocks:
  - **Hourly facts** (`int_city_hourly_*`) — **incremental**, deduped to one row per observation
    `(city, observed_at)`. Append-only, so they accumulate clean hourly history forever,
    independent of raw pruning. The durable archive.
  - **Forecast issue history** (`int_city_weather_forecast`) — incremental, every prediction as
    issued; the building block the forecast marts consume.
- **`marts`** — ✅ built. Dimensions (`dim_city` *derived, no seed* / `dim_date` / `dim_hour`),
  daily facts (`fct_*_daily`), hourly facts (`fct_traffic_hourly`, `fct_weather_hourly`, `fct_pollution_hourly`), `fct_forecast_accuracy`, the derived OBT
  `mart_city_daily`, and analytics marts (`mart_forecast_latest`, `mart_temperature_trends`,
  `mart_weather_alerts`). Star keys with `relationships` tests enforcing FK→dimension integrity.

| Layer | Tool | Location | Purpose |
|---|---|---|---|
| Ingestion | Airbyte (abctl) | localhost:8000 | API connectors, raw data load |
| Landing DB | PostgreSQL 18 | localhost:5432 | staging (raw JSON) + intermediate + marts schemas |
| Transformation | dbt (Python venv313) | — | staging ephemeral parsing (stg_*) + intermediate (hourly facts + forecast history) + marts (star + OBT), tests |
| Orchestration | Airflow (Docker) | localhost:8080 | DAG scheduling, automated pipeline + daily maintenance |

---

## Python Environment

**Always use `venv313` (Python 3.13) — NOT the old `venv` (Python 3.8).**
The old venv has incompatible dbt pins and will error on startup.

```bash
# Activate from project root
source venv313/Scripts/activate

# Or with full path from anywhere
source /c/Users/Andrej/Desktop/IWCONNECT-PRAKSA/smart-city-iw/venv313/Scripts/activate
```

---

## Running dbt (manually)

Always run from `dbt/smart_city/`. One target: `staging` → PostgreSQL (holds all schemas).

```bash
cd dbt/smart_city

# Install pinned dbt packages (dbt_utils 1.4.1 via package-lock.yml) — required once, and after
# any packages.yml change. Every model's surrogate keys use dbt_utils.generate_surrogate_key.
dbt deps

# Compile staging (stg_* are ephemeral — no DB object; builds nothing physical, just validates)
dbt run --select staging --target staging

# Build + test intermediate tables (hourly facts + forecast history)
dbt build --select intermediate --target staging

# Everything (staging → intermediate, in dependency order)
dbt build --select staging intermediate --target staging
```

`dbt build` runs models **and** their tests; `dbt run` builds without testing. (Once you
build the marts per `docs/marts_build_guide.md`, add `dbt build --select marts` to the
sequence. No `dbt seed` step — `dim_city` is derived from data, not a CSV.)

> Host runs **dbt-core 1.11.11 + dbt-postgres 1.8.2** and reads `~/.dbt/profiles.yml` (localhost).
> Because a `profiles.yml` also lives in the project dir (for Airflow/Docker, needs
> `SMART_CITY_PG_*` env vars), pass `--profiles-dir C:/Users/Andrej/.dbt` when running on the host
> so it doesn't pick up the container profile.
>
> **Keep the Airflow container's dbt on the same version.** `airflow/Dockerfile` pins the container's
> dbt to `dbt-core==1.11.11` / `dbt-postgres==1.8.2` to match the host — because dbt 1.9+ writes a
> `name:` key into each `package-lock.yml` entry that older dbt can't parse. An older container dbt
> (1.8.2) made the container's `dbt deps` (the one-time volume populate) fail with *"packages.yml is
> malformed"* (exit 2) on the host-generated lock. Host + container on the same version keeps the
> committed lock readable on both.

---

## APIs

**OpenWeather Free 2.5** (`OPENWEATHER_API_KEY`)
| Endpoint | Stream | Fields |
|---|---|---|
| `/data/2.5/weather` | `current_weather` | temp_celsius, humidity, wind_speed, pressure, weather_main, rain_1h |
| `/data/2.5/air_pollution` | `air_pollution` | aqi (1-5), pm2_5, pm10, co, no2, o3, so2, nh3 |
| `/data/2.5/forecast` | `weather_forecast` | forecast_dt, temp, pop (rain probability), weather_main |

**TomTom Traffic** (`TOMTOM_API_KEY`)
| Endpoint | Stream | Fields |
|---|---|---|
| `/traffic/services/4/flowSegmentData` | `traffic_flow` | currentSpeed, freeFlowSpeed, congestion_score, frc |
| `/traffic/services/5/incidentDetails` | `traffic_incidents` | id, delay, magnitudeOfDelay, geometry |

---

## Database Layout

### PostgreSQL — ingestion/landing

| Schema | Tables | Owner |
|---|---|---|
| `config` | sources, streams, locations, source_locations, field_mappings, validation_rules, validation_runs, load_errors | metadata-driven config — **DB-owned, no DDL in the repo**; reference + backup/restore in `metadata/README.md`. Single source of truth for ingestion + the data contract |
| `staging` | current_weather, air_pollution, weather_forecast, traffic_flow, traffic_incidents (raw JSON) | Airbyte |
| _(ephemeral, no DB object)_ | stg_current_weather, stg_air_pollution, stg_weather_forecast, stg_traffic_flow, stg_traffic_incidents | dbt (ephemeral CTEs — compile inline) |
| `intermediate` (hourly facts) | int_city_hourly_weather, int_city_hourly_pollution, int_city_hourly_traffic_flow, int_city_hourly_traffic_incidents | dbt (incremental tables) |
| `intermediate` (forecast) | int_city_weather_forecast | dbt (incremental issue history) |
| `marts` | dim_city, dim_hour, dim_date, fct_weather_daily, fct_pollution_daily, fct_traffic_daily, fct_traffic_incidents, fct_traffic_hourly, fct_weather_hourly, fct_pollution_hourly, fct_forecast_accuracy, mart_city_daily, mart_forecast_latest, mart_temperature_trends, mart_weather_alerts, mart_pollution_alerts | dbt (8 incremental `delete+insert` facts + 7 tables — see Marts materialization) |
| `marts` (AI) | mart_city_summary | **Not dbt** — AI-generated daily narratives, written by `ai/load_summaries.py` (Gemini via the `@daily` `smart_city_ai_summary` DAG; Claude Code as manual fallback — the `model` column says which). FK `city_date_key`/`city_key`/`date_key` into the star. |
| `ml_predictions` | aqi_forecast, temperature_forecast, traffic_forecast, rain_forecast, city_score_forecast, pollution_anomaly, model_registry (+ views `model_health`, `all_predictions`) | **Not dbt** — ML forecasts written by `ml/predict.py` via the `@daily` `smart_city_ml` DAG. DDL in `ml/schema.sql`. Every table carries `city_key`/`date_key` into the star — `city_key` **joined from `dim_city`**, never re-hashed (only dbt calls `generate_surrogate_key`); `date_key` is `YYYYMMDD::int`, computed. **Power BI: import the `all_predictions` view only** (six tables sharing `city` would re-trip the autodetect cyclic error) and relate `city_key` → `dim_city`, leaving `date_key` unrelated like `mart_forecast_latest` — forecasts point at future dates a date filter would blank. |

**Hourly facts grain & keys:** one row per clock hour. Each model dedupes its staging source on the
stream's business key — `(city, date_trunc('hour', observed_at))` for weather/pollution/flow (key
`city_hour_key`), keeping the **freshest reading in the hour** (`order by observed_at
desc, extracted_at desc`); `(city, incident_id, observed_at)` for incidents (key `city_incident_key`,
with `where incident_id is not null`). All surrogate keys are built with
`dbt_utils.generate_surrogate_key([...])` over those columns (was hand-written `md5(a || '|' || b)`).
Hour-truncating both the partition and the key means two syncs in one clock hour collapse to a single
row (idempotent across runs). `materialized='incremental'`, `delete+insert`, 6h lookback; carries
`date_utc` + `hour_utc` for time-of-day analysis. `unique`/`not_null` tests on the surrogate key.

**Marts grain & keys:** daily facts + OBT one row per `(city, date_utc)`, surrogate
`city_date_key = generate_surrogate_key(['city','date_utc'])`; star keys
`city_key = generate_surrogate_key(['city'])`, `date_key = YYYYMMDD::int`;
`relationships` tests enforce FK→dimension integrity.
`dim_city` is **derived** from data (weather facts + traffic presence), not a seed.
`dim_date` is an **independent** calendar spine (fixed 2026-01-01 anchor → `current_date + 365d`,
not bounded by the facts) so the dims resolve first; the fixed anchor still guarantees every
fact `date_key` exists in the dimension. `dim_hour` carries `hour_label` (`'06:00'`) + `day_part`.
`mart_city_daily` LEFT-joins weather+pollution+traffic so weather-only cities (Skopje, Prilep,
Bitola, Ohrid) appear with NULL traffic. Full spec + reference SQL in `docs/marts_build_guide.md`.

**Marts materialization (mixed, since 2026-07-20):** the 8 **append-only** facts are
`materialized='incremental'`, `delete+insert` (3 hourly on `city_hour_key`, 3 daily on
`city_date_key`, `fct_forecast_accuracy` on `forecast_key`, `mart_pollution_alerts` on
`alert_key`). The other 7 stay `table` on purpose — dims (tiny/static), the two rolling-window
marts (`mart_city_daily`, `mart_temperature_trends` — windows need prior days as input rows, so
an incremental batch would truncate them), and the two forward-looking snapshots
(`mart_forecast_latest`, `mart_weather_alerts` — passed slots must drop out, which `delete+insert`
can't express). Column shapes are unchanged, so the Power BI (PBIP) import contract is preserved.
`dbt build --select marts --full-refresh` rebuilds all identically if keys ever drift.

dbt project root: `dbt/smart_city/`
Profiles: `~/.dbt/profiles.yml` (host) + `dbt/smart_city/profiles.yml` (Docker/Airflow)
Targets: `staging` → PostgreSQL (only)
Plan/design doc for the marts: `docs/marts_implementation_plan.md`

---

## Airbyte Setup

### Deployment
- Installed via `abctl` (Kubernetes/Kind), not docker-compose
- UI: `localhost:8000`
- Kubeconfig: `~/.airbyte/abctl/abctl.kubeconfig`

### Config-Driven Setup

```bash
# Set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET in .env first
python ingestion/scripts/setup_airbyte.py
```

Outputs `ingestion/config/connection_ids.yml` with connection UUIDs for Airflow.

Since 2026-07-22 `setup_airbyte.py` reads sources/streams/cities from the **`config` schema in
Postgres** (`config.sources` / `config.streams` / `config.source_locations`), not YAML. Two
entrypoints: `main()` (host — manages the Postgres destination + pushes this machine's LAN IP;
run after a network switch) and `reconcile()` (container-safe — skips the destination, called by
the DAG's `reconcile_airbyte` task).

**One source + connection per API**, not per city: `openweather_all` and `tomtom_all`.
Each connector is partition-routed (`ListPartitionRouter`) over a `locations` array — one API
request per city per stream, all inside one sync. **Add a city** = `select config.add_city('Zagreb',
lat, lon [, bbox])` (helper does the `config.locations` + `config.source_locations` inserts; also
`config.set_city_active(city, bool)` and `config.remove_city(city)` — see `metadata/README.md`),
then the next `reconcile_airbyte` run (or `python ingestion/scripts/setup_airbyte.py` on the host)
applies it; no new connection, no DAG re-parse. The old `sources.yml`/`connections.yml` were
deleted 2026-08-10 along with the seeder that was their last reader.

Config source of truth: the `config` schema **in Postgres** (no DDL in the repo — `metadata/README.md`)
Connector YAMLs: `ingestion/connections/open_weather_free_2_5.yaml`, `ingestion/connections/tomtom_traffic.yaml`

### Auth
Airbyte API uses OAuth application tokens (not basic auth).
Get `client_id` / `client_secret` from Airbyte UI → User → Applications.
Set `AIRBYTE_CLIENT_ID` and `AIRBYTE_CLIENT_SECRET` in `.env`.

> **Short-lived tokens — poll loop re-auths.** Application access tokens expire in minutes.
> `airbyte_utils.py` caches the token in a module global, so a long sync (> token TTL)
> outlives the token cached at the start of `wait_for_sync`, and mid-poll the `jobs/get`
> call 401s. Because `HTTPError` subclasses `RequestException`, the poll loop's transient
> handler catches the 401 too — it now detects 401/403, clears the cached token so the next
> `_headers()` re-authenticates, and retries (instead of spinning on the dead token until the
> task's `execution_timeout` kills it — a 401 that looked like a slow sync). `wait_for_sync`'s
> default `timeout` is `2100`s (35 min), under the `sync_*` task's 45-min `execution_timeout`,
> so its own `TimeoutError` (which names the `job_id`) surfaces before Airflow's generic kill.

### Known quirks
- Destination host must be LAN IP (`AIRBYTE_PG_HOST`) — not localhost (sync pods run in Kind).
  **The LAN IP changes when you join a different network**, and Airbyte stores it *literally*,
  so every sync fails from a new network until the destination is re-pointed (this bit us
  2026-07-14: both connections failed all evening from home, then "fixed themselves" back at
  the office). Now handled: `AIRBYTE_PG_HOST=auto` auto-detects the default-route IP and
  `setup_airbyte.py` **pushes** it to the existing destination. **After switching networks,
  re-run `python ingestion/scripts/setup_airbyte.py`** — that's the whole procedure.
  Postgres's side is already network-agnostic (`pg_hba.conf` uses `samenet`, see below).
- Schema refresh may 403 on connector version change — delete and recreate the connection instead
- `city` column injected via `AddFields` — old rows synced before connector update have NULL city (filter with `WHERE city IS NOT NULL` in any downstream model that aggregates by city)
- TomTom incidentDetails v5 returns only `iconCategory` + geometry **unless** the `fields` query param lists the attributes — the `traffic_incidents` requester now sends it (fix). Editing the repo YAML alone has no effect: the connector must be **republished in the Airbyte Builder UI** to take effect.

---

## Airflow

### Starting Airflow
```bash
cd airflow
docker compose up -d     # start all services
docker compose down -v   # full teardown (wipes DB)

# First time setup (after teardown):
docker compose run --rm airflow-init
docker compose up -d
```

UI: `localhost:8080` — login: `admin / admin`

### DAG: `smart_city_pipeline`
- Schedule: `@hourly`
- `max_active_runs=1` — **runs are serialized.** Worst-case duration (syncs 45m +
  the three dbt steps 15m each) can exceed the hourly interval; without this the scheduler
  would start the next run while the current one is still writing, so two
  `dbt_intermediate`/`dbt_marts` tasks would `DELETE+INSERT` the same incremental Postgres
  tables concurrently (deadlocks / lost rows). `=1` queues the next run; `catchup=False`
  means a long run skips ahead rather than piling up.
- **First task `reconcile_airbyte` (auto-detect)** — runs `setup_airbyte.reconcile()` so new
  sources/cities in `config.*` are applied to Airbyte before syncing. **Best-effort**: imports
  inside the task (can't break DAG parsing) and never raises (can't block ingestion — a real
  Airbyte outage surfaces on the sync tasks). Skips the destination (LAN-IP detection is host-only).
- Syncs all Airbyte connections in parallel — one `syncs.sync_*` task per connection in
  `connection_ids.yml` (now 2: `openweather_all`, `tomtom_all`) **triggers its sync and waits for
  it in the same task**. Trigger + wait were merged (2026-07-20; was a `trigger_syncs`/`wait_syncs`
  split that passed `job_id` via XCom) so an Airflow retry re-triggers a *fresh* sync instead of
  re-polling an already-failed job — see the Recently Completed note above.
- **`validate_contract` gate (after syncs, before dbt; `retries=0`)** — `config_utils.validate_streams`
  checks the latest raw batch against `config.field_mappings` (required) + `config.validation_rules`.
  Stops the pipeline (raises → `on_failure` email lists the failing field/threshold) on any
  error-severity breach; logs every check to `config.validation_runs` (committed before the raise).
  `warn`-severity breaches log but don't stop. So bad/missing data never reaches intermediate/marts.
- **No per-run `dbt deps` step** (removed 2026-07-20). `dbt_utils` (1.4.1) lives in the persistent
  `dbt_packages` **named volume** declared in `docker-compose.yml` (layered over the `../dbt` bind
  mount at the `dbt_packages/` subpath), populated **once** via a manual `dbt deps` and then durable
  across restarts/rebuilds. `dbt_packages/` is gitignored + the project is bind-mounted, so the image
  can't bake it in (the mount would shadow it) — the named volume is the stable alternative, and it
  keeps a registry/network call off the hourly critical path. **One-time populate** (re-run the same
  command after a `docker compose down -v` or any `packages.yml` change):
  ```bash
  docker compose run --rm --user root \
    --entrypoint /home/airflow/dbt_venv/bin/dbt airflow-scheduler \
    deps --project-dir /opt/airflow/dbt/smart_city --profiles-dir /opt/airflow/dbt/smart_city
  ```
  If the volume is empty (never populated / wiped by `-v`), the first model run fails with
  "dbt_utils not found" — repopulate with the command above. Two non-obvious flags, learned
  the hard way when this was set up: **`--entrypoint`** (the airflow image otherwise passes the
  args to its `airflow` CLI → "invalid choice"), and **`--user root`** (a fresh named volume is
  root-owned; root can create the install dir, and dbt's files come out world-readable so the DAG's
  airflow user reads them fine). And the container installs into the **`dbt_packages/lib` subdir** of
  the volume via `DBT_PACKAGES_PATH` (set in `docker-compose.yml` + `dbt_project.yml`'s
  `packages-install-path`), because `dbt deps` rmtree's its own install path and **can't remove a
  volume's mount root** (Errno 16 "Device or resource busy"). The **host** `dbt deps` is unaffected
  (env var unset → default `dbt_packages`).
- Runs `dbt run --select staging --target staging` — the `stg_*` are now **config-driven**:
  each is `{{ build_staging('<stream>') }}`, generated from `config.field_mappings` at run time
  (the `build_staging`/`get_field_mappings` macros). Output is byte-identical to the old
  hand-written models (verified), so downstream is unaffected.
- Runs `dbt build --select intermediate --target staging` (hourly facts + forecast history)
- Runs `dbt build --select marts --target staging` (star schema + OBT + analytics, build+test)
- **Email alerts:** `on_failure_callback` on every task (fires after retries — emails which
  step failed + the error); `on_success_callback` on the final `dbt_marts` task (one
  whole-pipeline SUCCESS email). Both guarded by `ALERT_EMAIL`; no-op if unset.

### DAG: `smart_city_maintenance`
- Schedule: `@daily`
- `max_active_runs=1` — serialized, so a slow prune can't overlap the next day's (both
  `DELETE` from the same `staging` tables and would race the pipeline's reads).
- Cleans up old `staging` (raw JSON) rows per retention policy (`RETENTION_DAYS`)
- Decoupled from the ELT pipeline so pruning runs regardless of any individual
  ELT run. Safe because deduped history is preserved downstream in the
  incremental `int_city_hourly_*` tables (raw is a short 1-day buffer).
- **Email alerts:** same pattern — failure email on the cleanup task, success email confirming
  the daily prune ran clean.

### DAG: `smart_city_ai_summary`
- Schedule: `@daily` — `fetch_pack` → `generate` → `load` (the three `ai/` steps, **imported**
  as modules so failures surface as real tracebacks in the task log + alert email).
- **Only `generate` calls a model** (Gemini `generateContent`, one POST for all 10 cities). Both
  ends are deterministic SQL, so a bad generation is re-runnable without touching the warehouse.
- **Summarizes yesterday**: `@daily` + `catchup=False` ⇒ `ds` is the complete UTC day the interval
  covers, whose `mart_city_daily` row is final. If the dev machine was off at midnight, the
  scheduler runs that same interval when it next comes up (no skipped days on a laptop host).
- **Skips (not fails) when there are no marts rows for the date** — a day the ELT never ran isn't
  an error worth an alert email. `AirflowSkipException` from `fetch_pack`.
- `max_active_runs=1` — two runs would upsert the same `city_date_key`s concurrently and burn
  double the API quota for the same output.
- Backfill a single day: *Trigger DAG w/ config* → `{"date": "2026-07-29"}`.
- Needs the `../ai` mount + `GEMINI_MODEL` in `docker-compose.yml` (`GEMINI_API_KEY` comes through
  the existing `env_file: ../.env`). Same `alert_utils` failure/success emails as the other DAGs.
### DAG: `smart_city_ml`
- Schedule: `@daily` — `train` → `predict` → `report`.
- **Runs in its own `ml_venv`, invoked as a subprocess** (`/home/airflow/ml_venv/bin/python`),
  exactly like the dbt tasks call `dbt_venv`'s binary. airflow 2.9.3 pins pandas/numpy and
  scikit-learn/xgboost want their own — the same dependency tug-of-war that forced dbt into a
  separate venv. `_run()` captures the child's output and puts its tail into the raised
  `AirflowException`, so the alert email still says what broke instead of just "exit 1".
- **Retrains every run.** Models are build artifacts (`ml/_models/`, gitignored); at ~1,700 rows
  training takes seconds, so retraining beats distributing `.joblib` files and the models never
  drift from a stale snapshot. Split into weekly-train + daily-predict if that ever costs real time.
- **Kept out of the hourly pipeline** — horizons are 1h–1d and `city_score` is daily-grain, so
  hourly runs would rewrite the same forecasts 24×/day off partial data.
- **A model losing to its naive baseline does NOT fail the DAG** (passes
  `--allow-worse-than-baseline`). "24h PM2.5 is hard on 7 weeks of gappy data" is a property of the
  data, not an incident; a daily email about it would train everyone to ignore alerts. Skill is
  surfaced via `ml_predictions.model_health`, and the `report` task logs the standings so a genuine
  regression stays visible. `predict` **does** fail if it wrote 0 rows (forecasts silently stale).
- `max_active_runs=1` — two runs would fit the same models and upsert the same keys.
- Needs the `../ml:/opt/airflow/ml` mount **and** a `docker compose build` (for `ml_venv`).

### Email alerts (both DAGs)
Both DAGs share `airflow/dags/alert_utils.py` — `on_failure` (attached to every task via
`default_args`) and `make_success_callback(message)` (attached to the DAG's **last** task only, so
it means "the whole pipeline finished clean"). The logic used to be copy-pasted in both DAGs, so
every fix had to land twice.
Failure/success notifications go to `ALERT_EMAIL` via `airflow.utils.email.send_email`. SMTP is
configured entirely through `AIRFLOW__SMTP__*` env vars (no `airflow.cfg` edit) — Gmail SMTP with a
16-char **App Password** (Google Account → Security → 2-Step Verification → App passwords), *not*
the account login. Callbacks are guarded by `if ALERT_EMAIL:`, so leaving it unset disables email
without breaking the DAGs. Each email leads with the run's **actual** wall-clock window
(`Started` → `Finished`/`Failed`, with duration) in local time (`ALERT_TZ`, default
`Europe/Skopje`), then the Airflow **logical date** labelled `UI label` (the data-interval start
the UI's *Last Run* shows, ~1h behind for `@hourly`) so it reconciles instead of reading as a
contradiction, and finally the raw `run_id` as a small footer. Built by `_run_block_html` in
`alert_utils.py`, shared by both callbacks (was a bare `run_id` + `Completed`/`Failed at` stamp —
the raw `run_id` was unreadable). On an eventual Airflow 3 upgrade, move the SMTP creds into an `smtp_default`
connection (env-var creds are deprecated there).

**Sync-failure emails explain *why*.** A failed Airbyte sync used to email only `Airbyte job N
ended with status: failed`, which couldn't tell a network problem from a bad API key.
`wait_for_sync` now reads the failure detail Airbyte already returns in the `jobs/get` payload
(`attempts[].attempt.failureSummary.failures[]`) and raises with `failureOrigin` /
`failureType` / the messages, plus a plain-English hint for common causes (Postgres
unreachable → re-run `setup_airbyte.py`; `no pg_hba.conf entry`; bad password; rejected API
key; rate limit). Unmatched failures still show their raw message — the hint map never hides
detail. Java stacktraces go to the **task log only**, never the email. The callbacks render the
error in `<pre>` + `html.escape` (`_error_html`) because the detail is multi-line and a plain
`<p>` collapsed it into one run-on.

### Airflow env vars (from `airflow/.env` and docker-compose)
| Var | Purpose |
|---|---|
| `SMART_CITY_PG_HOST` | `host.docker.internal` — PostgreSQL from inside Docker |
| `SMART_CITY_PG_PASSWORD` | PostgreSQL password |
| `AIRBYTE_URL` | `http://host.docker.internal:8000` |
| `AIRBYTE_CLIENT_ID` | Airbyte OAuth client ID |
| `AIRBYTE_CLIENT_SECRET` | Airbyte OAuth client secret |
| `GEMINI_API_KEY` | Google AI Studio key — the model call in the daily `smart_city_ai_summary` DAG (via `env_file: ../.env`) |
| `GEMINI_MODEL` | Gemini model for the summaries (default `gemini-3.6-flash`; **not** 2.5-flash — retired for new keys) |
| `ALERT_EMAIL` | Recipient(s) for pipeline failure/success emails — comma-separate for several (unset = email disabled) |
| `ALERT_TZ` | Optional — tz for the email "Completed"/"Failed at" stamp (default `Europe/Skopje`, UTC fallback) |
| `AIRFLOW__SMTP__SMTP_HOST` … `_MAIL_FROM` | SMTP config (Gmail + App Password); see Environment Variables |

---

## Environment Variables

```
# PostgreSQL (used by dbt staging target + host applications)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=smart_city
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<your password>

# APIs
OPENWEATHER_API_KEY=<from openweathermap.org>
TOMTOM_API_KEY=<from developer.tomtom.com>

# Airbyte
AIRBYTE_PG_HOST=auto       # auto-detect LAN IP (or pin an explicit IP) — NEVER localhost
AIRBYTE_URL=http://localhost:8000
AIRBYTE_USERNAME=<your email>
AIRBYTE_PASSWORD=<your password>
AIRBYTE_CLIENT_ID=<from Airbyte UI → User → Applications>
AIRBYTE_CLIENT_SECRET=<from Airbyte UI → User → Applications>
AIRBYTE_WORKSPACE_ID=<from Airbyte UI URL>

# Email alerts (Airflow reads AIRFLOW__SMTP__* straight from env)
ALERT_EMAIL=<inbox for pipeline alerts>   # one address, or several comma-separated
ALERT_TZ=Europe/Skopje   # optional — tz for the "Completed" stamp (UTC fallback)
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=<your gmail>
AIRFLOW__SMTP__SMTP_PASSWORD=<16-char Gmail App Password>
AIRFLOW__SMTP__SMTP_MAIL_FROM=<your gmail>

# Gemini — the model call behind the daily AI city summaries (smart_city_ai_summary DAG)
GEMINI_API_KEY=<from https://aistudio.google.com/api-keys>
GEMINI_MODEL=gemini-3.6-flash        # optional — NOT gemini-2.5-flash (retired for new keys)
GEMINI_THINKING_LEVEL=LOW            # optional — Gemini 3+ (LOW/MEDIUM/HIGH)
GEMINI_THINKING_BUDGET=0             # optional — Gemini 1.x/2.x only (0 = thinking off)
GEMINI_MAX_OUTPUT_TOKENS=8192        # optional
```

---

## Key Constraints

- Always use `venv313` (Python 3.13) — old `venv` (Python 3.8) has incompatible dbt pins
- PostgreSQL runs locally (not Docker) on port 5432
- `AIRBYTE_PG_HOST` must be LAN IP — Airbyte pods can't reach host `localhost`. Leave it at
  `auto` and re-run `setup_airbyte.py` after switching networks
- `pg_hba.conf` uses `host all all samenet scram-sha-256` — accepts any subnet this machine is
  directly attached to, so Postgres needs no edit per network. **Host config, not in git** —
  a rebuilt machine must redo it (`SELECT type, address, auth_method FROM pg_hba_file_rules;`
  to check; `SELECT pg_reload_conf();` to apply)
- **The `config` schema is host state, not in git** — same category as `pg_hba.conf` and `.env`
  (changed 2026-08-10; the DDL + seeder were deleted). A rebuilt machine restores it from
  `Documents/smart_city_config_backups/` (`pg_dump --schema=config`) **before** the first
  `dbt build`, or staging fails — `build_staging` reads `config.field_mappings` at run time.
  Re-dump after any material config change. Reference: `metadata/README.md`
- Airflow runs in Docker (not natively on Windows)
- dbt runs in `venv313` on the host machine (manual) OR inside Airflow container (automated)
- All timestamps stored as UTC
- Never manually edit the raw tables in `staging` (current_weather, air_pollution, …) — Airbyte owns them
- `city` column injected by Airbyte `AddFields` — rows before this change have NULL city (filtered out)
- `airflow/.env` must exist with POSTGRES_PASSWORD, AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET

---

## Folder Structure

```
smart-city-iw/
├── ingestion/
│   ├── config/
│   │   └── connection_ids.yml   ← auto-generated, git-ignored (dir kept via .gitkeep
│   │                               for the docker-compose mount; the old sources.yml +
│   │                               connections.yml were deleted 2026-08-10)
│   ├── connections/
│   │   ├── open_weather_free_2_5.yaml
│   │   └── tomtom_traffic.yaml
│   ├── scripts/
│   │   └── setup_airbyte.py
│   └── README.md
├── airflow/
│   ├── Dockerfile               ← extends apache/airflow:2.9.3 with dbt
│   ├── docker-compose.yml
│   ├── .env                     ← POSTGRES_PASSWORD, AIRBYTE_* (not committed)
│   └── dags/
│       ├── airbyte_utils.py     ← OAuth trigger/wait helpers + sync-failure diagnosis
│       ├── alert_utils.py       ← shared failure/success email callbacks (both DAGs)
│       ├── config_utils.py      ← config-schema reads + data-contract validation engine
│       ├── dag_smart_city_pipeline.py      ← hourly ELT (reconcile → sync → validate → dbt)
│       ├── dag_smart_city_maintenance.py   ← daily raw cleanup
│       ├── dag_smart_city_ai_summary.py    ← daily AI city summaries (Gemini): fetch → generate → load
│       └── dag_smart_city_ml.py            ← daily ML train → predict → report
├── dbt/
│   └── smart_city/              ← dbt project root (run dbt here)
│       ├── dbt_project.yml
│       ├── profiles.yml         ← Docker/Airflow profiles (container paths)
│       ├── packages.yml         ← dbt package deps (dbt_utils); package-lock.yml pins 1.4.1
│       ├── macros/              ← generate_schema_name.sql; backfill_surrogate_keys.sql
│       │                          (idempotent key repair, run manually — never auto-runs);
│       │                          build_staging.sql + get_field_mappings.sql (config-driven stg_*)
│       └── models/
│           ├── staging/         ← 5 stg_* → one-line {{ build_staging('<stream>') }}, generated
│           │                       from config.field_mappings; ephemeral (inline CTEs, no DB object)
│           ├── intermediate/    ← hourly facts (4) + forecast history (1) → tables
│           └── marts/           ← 15 models: dims + facts (incl. hourly weather/pollution) + OBT + analytics → tables
├── docs/                        ← ⚠️ LOCAL-ONLY, gitignored. The repo ships only docs/.gitkeep —
│   │                              a fresh clone has NONE of the files below. The READMEs (root,
│   │                              ingestion/, dbt/smart_city/) are the shipped docs and must stay
│   │                              self-contained: never link a README to anything in here.
│   ├── staging_as_raw_landing.md     ← airbyte_raw→staging collapse: ephemeral parsing, JSON→typed
│   ├── marts_build_guide.md          ← marts build walkthrough + reference SQL
│   ├── marts_implementation_plan.md  ← marts star-schema design / rationale
│   ├── powerbi_dashboard.md          ← Power BI build log
│   ├── powerbi_dashboard_plan.md     ← Power BI page-by-page plan
│   ├── deployment.md                 ← deployment notes
│   └── branch-reconciliation.md      ← branch reconciliation notes
├── metadata/                    ← ✅ SHIPPED (committed). Documentation ONLY — the `config`
│   └── README.md                     ← the config schema's table reference (8 tables, all columns
│                                       + constraints), inspect/backup/restore, what-still-needs-code.
│                                       ⚠️ NO DDL: the DB owns the schema (renamed from config/ +
│                                       schema.sql/migrations.sql/seed_config.py deleted 2026-08-10)
├── ai/                          ← ✅ SHIPPED (committed). AI city summaries — scheduled daily by
│   │                              the smart_city_ai_summary DAG (Gemini API):
│   ├── README.md                     ← the shipped guide (pipeline, scheduling, config, rationale)
│   ├── common.py                     ← shared get_conn() (POSTGRES_* on host / SMART_CITY_PG_* in container)
│   ├── fetch_inputs.py               ← Step A: DDL + read mart_city_daily → context pack (build_pack())
│   ├── summary_spec.md               ← the generation rules — SINGLE source of truth, both paths read it
│   ├── generate_summaries.py         ← Step B: the Gemini call (requests, structured output) → rows
│   ├── PROMPT.md                     ← Step B fallback: same step in a Claude Code session (no API key)
│   ├── load_summaries.py             ← Step C: upsert rows → marts.mart_city_summary (upsert())
│   └── _inputs/ , _outputs/          ← gitignored scratch (context packs + generated summaries)
├── ml/                          ← ✅ SHIPPED (committed). ML prediction pipelines —
│   │                              scheduled daily by the smart_city_ml DAG:
│   ├── README.md                     ← the shipped guide (models, skill table, constraints)
│   ├── schema.sql                    ← ml_predictions DDL (idempotent, auto-applied)
│   ├── common.py                     ← DB conn (host/container), model IO, stable city encoding
│   ├── featurelib.py                 ← leak-free lags (backward) + guarded future targets
│   ├── evaluate.py                   ← chronological split + naive-baseline skill gate
│   ├── pipelines.py                  ← the 6 pipelines declared as data
│   ├── train.py / predict.py         ← the two CLIs (generic over pipelines.py)
│   └── _models/                      ← gitignored build artifacts (*.joblib)
├── venv313/                     ← Python 3.13 venv (use this one)
├── venv/                        ← Python 3.8 venv (legacy, do not use)
├── requirements.txt
├── .env
└── .env.example
```
