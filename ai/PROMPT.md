# City-summary generation — Claude Code / subscription path (manual fallback)

The **scheduled** path is `ai/generate_summaries.py` (Gemini API, run daily by the
`smart_city_ai_summary` Airflow DAG). This file is the **manual fallback**: the same
generation step performed inside a Claude Code session, so no API key or billing is
involved. Use it when the Gemini key is missing/rate-limited, or to hand-check a day.

## Task

1. Read the context pack at `ai/_inputs/<target_date>.json` (produced by
   `ai/fetch_inputs.py`).
2. Write the summaries **exactly per [`ai/summary_spec.md`](summary_spec.md)** — that
   file holds all the content rules (word count, ordering, grounding, the
   weather-only-city clause) and is shared with the Gemini path. Follow it as written.
3. Write the result to `ai/_outputs/<target_date>.json` as the JSON list the spec
   describes.
4. Load it: `python ai/load_summaries.py --date <target_date> --model "claude-code"`.

Nothing else on this page overrides the spec — if the two ever disagree,
`ai/summary_spec.md` wins.
