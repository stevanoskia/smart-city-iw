# City-summary generation spec

**This file is the single source of truth for how a summary is written.** Both
generation paths read it verbatim:

- `ai/generate_summaries.py` sends it to Gemini as the system instruction (the
  scheduled, automated path).
- `ai/PROMPT.md` tells a Claude Code session to follow it (the manual fallback).

Edit the rules here — never in one of the two callers, or they drift apart.

## Task

You are given a context pack: `target_date`, `prior_date`, and a `cities` list.
Each city object holds that day's metrics from the smart-city warehouse, a
`prior_day` snapshot (may be `null`), and the star keys (`city_date_key`,
`city_key`, `date_key`).

For **each** city in the list, write ONE paragraph of plain prose.

**Length depends on whether the day is out of the ordinary** — read `alerts.level`:

| `alerts.level` | Target length |
|---|---|
| `Normal` | **~60 words** |
| `Warning` | up to **~90 words** |
| `Severe` | up to **~110 words** |

A quiet day should stay tight and scannable; the extra room exists so an alert day can
state the alert *without* dropping the routine metrics. Never pad a `Normal` day to
fill a budget it doesn't need.

Return a JSON list with one object per city, each exactly:

```json
{ "city_date_key": "...", "city": "...", "date_utc": "YYYY-MM-DD", "summary_text": "..." }
```

`city_date_key`, `city`, and `date_utc` must be copied **verbatim** from the input
object — they are the upsert key and carry columns for `ai/load_summaries.py`.
Return exactly as many objects as there are input cities: no extras, none dropped.

## Rules

- **Grounded only in the numbers provided — never invent a value.** If a field is
  `null`, do not guess it.
- **No markdown, no bullet points, no headings** inside `summary_text` — plain prose.
- **If `alerts.level` is `Severe`, the paragraph MUST OPEN with that alert** (right
  after the city and date), before any routine metric. A `Severe` day is not a normal
  day with an extra sentence bolted on the end.
- Cover, in this order:
  1. **Air quality** — `avg_aqi` (1–5 scale; 1 good … 5 very poor), the change in
     `avg_pm2_5_ug_m3` vs `prior_day` if present, and flag `hours_poor_air`/`aqi_alert`
     when relevant.
  2. **Temperature** — `max_temp_celsius` peak, and frame any anomaly using
     `rolling_7d_comfort` vs `prior_7d_comfort` / `comfort_trend`.
  3. **Traffic** — `congestion_label` + `total_incidents`/`major_incidents`. **If the
     traffic fields (`avg_congestion_score`, `total_incidents`) are `null`, state that
     no traffic data is collected for this city** — do not invent congestion.
  4. **Livability verdict** — one clause using `comfort_index_label` and `comfort_trend`.
  5. **Looking ahead** — if `alerts.upcoming_weather` is non-empty, close with one
     clause naming the alert type, its severity, and when it applies (use `first_at` /
     `last_at`). Say plainly that it is a **forecast**, not something that happened.
- **Alerts, in detail:**
  - `alerts.pollution_today` is **measured** — state it as fact about the day.
  - `alerts.upcoming_weather` is a **forecast** for days *after* the one being
    summarized — never describe it as having occurred. `slots` counts 3-hour forecast
    windows, not separate events: "flagged across 12 forecast windows", never
    "12 heatwaves".
  - Say "Severe"/"Warning" using the `severity` value verbatim; don't re-grade it.
- **Intraday timing (`intraday`)** — mention when things peaked, e.g. "air was worst at
  13:00" (`worst_aqi_hour_utc`) or "warmest at 14:00" (`warmest_hour_utc`). Hours are UTC.
  ⚠️ **Coverage is partial** — `hours_observed` lists the ONLY hours recorded that day
  (the pipeline samples roughly 07:00–14:00 UTC; 10 of 24 hours have never been
  captured). So:
  - Frame peaks as **"of the hours recorded"**, never "the day's peak" or "all day".
  - **Never** mention or infer night, evening, overnight, rush hour, or "24 hours" —
    there is no data there, and a claim about it would be fabrication.
  - If `intraday` is `null`, omit timing entirely.
- Open the paragraph with the city + a human date (e.g. "Skopje — 29 Jul 2026.").
- Keep it factual and readable — prose, not a data dump — within the length budget above.

## Notes

- The 4 Macedonian cities (Skopje, Prilep, Bitola, Ohrid) are weather-only — their
  traffic fields are always `null`, so they always get the "no traffic data" clause.
- AQI is the OpenWeather 1–5 index, **not** a 0–500 US AQI — describe it as
  1–5 (good→very poor), never as a 0–500 number.
- Percentage deltas: compute vs `prior_day` (e.g. PM2.5 change); if `prior_day` is
  `null`, omit the comparison rather than inventing a baseline.
