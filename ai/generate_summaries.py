"""
Step B of the AI city-summaries workflow — the model call (Gemini API).

Sends the context pack from step A to the Gemini `generateContent` endpoint and
returns/writes the summary rows step C upserts. This is the **scheduled** path,
run daily by the `smart_city_ai_summary` Airflow DAG; the Claude Code session
described in ai/PROMPT.md remains as a manual fallback.

Design notes:

* **Plain `requests`, not the google-genai SDK.** `requests` is already installed
  in both venv313 and the Airflow image, and the REST endpoint is a stable two-field
  POST. Adding an SDK to the Airflow env risks the same dependency tug-of-war that
  forced dbt into its own venv (see airflow/Dockerfile) — not worth it for one call.
* **One call for all cities, not one per city.** The free tier is rate-limited per
  minute; 10 sequential calls would burn the budget and take 10x as long. The
  response is validated for full coverage instead (see _validate).
* **Structured output** (`responseMimeType: application/json` + `responseSchema`)
  so the model returns a parseable list rather than prose we'd have to scrape.
* **Thinking kept minimal** — this is short, grounded writing, so thinking tokens
  are pure latency/cost. The field for it is **model-family-specific**: Gemini 3+
  takes `thinkingLevel` ("LOW"/"MEDIUM"/"HIGH"), Gemini 1.x/2.x takes a numeric
  `thinkingBudget` (0 = off) and **rejects `thinkingLevel`**, and vice-versa — sending
  the wrong one is a bare `400 INVALID_ARGUMENT` that names no field. _thinking_config()
  picks by model name, and generate() retries once WITHOUT thinkingConfig if the API
  rejects it, so a future rename degrades to a working call instead of a dead DAG.
* **Grounding is enforced by the prompt, not by the model** — see ai/summary_spec.md,
  which is sent verbatim as the system instruction and shared with the manual path.

CLI (venv313):
    python ai/generate_summaries.py [--date YYYY-MM-DD] [--model gemini-2.5-flash]
                                    [--from-file] [--dry-run]

Requires GEMINI_API_KEY in .env (host) or the container env.
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import argparse

import requests

try:  # importable both as a module (Airflow DAG) and as a script
    from common import (INPUTS_DIR, OUTPUTS_DIR, load_env, force_utf8_stdout, read_spec)
    import fetch_inputs
except ImportError:  # pragma: no cover - direct path import fallback
    from ai.common import (INPUTS_DIR, OUTPUTS_DIR, load_env, force_utf8_stdout, read_spec)
    from ai import fetch_inputs

load_env()
force_utf8_stdout()

API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"

# Pinned rather than the moving `gemini-flash-latest` alias, so a model swap is an
# explicit decision, not a silent change in how the summaries read. Do NOT drop to
# gemini-2.5-flash: it is retired for keys created after ~2026, which returns a 404
# "no longer available to new users" even though it still appears in ListModels.
DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")

# The shape step C expects. Gemini enforces this server-side, so a well-formed
# list comes back or the call errors — no prose-scraping.
RESPONSE_SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "city_date_key": {"type": "STRING"},
            "city": {"type": "STRING"},
            "date_utc": {"type": "STRING"},
            "summary_text": {"type": "STRING"},
        },
        "required": ["city_date_key", "city", "date_utc", "summary_text"],
        "propertyOrdering": ["city_date_key", "city", "date_utc", "summary_text"],
    },
}

RETRY_STATUS = {429, 500, 502, 503, 504}  # rate limit + transient server errors


class GenerationError(RuntimeError):
    """Raised when Gemini returns nothing usable — Airflow retries the task."""


def _api_key() -> str:
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not key:
        raise GenerationError(
            "GEMINI_API_KEY is not set. Add it to .env (host) or the Airflow "
            "container env — get one at https://aistudio.google.com/api-keys."
        )
    return key


def _post(model: str, body: dict, timeout: int, attempts: int = 4) -> dict:
    """POST to generateContent with backoff on rate-limit / transient errors."""
    url = f"{API_BASE}/{model}:generateContent"
    headers = {"x-goog-api-key": _api_key(), "Content-Type": "application/json"}

    last = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=timeout)
        except requests.RequestException as exc:  # connection reset, DNS, timeout
            last = f"request failed: {exc}"
        else:
            if resp.status_code == 200:
                return resp.json()
            # Surface the API's own message — it names the real cause (bad key,
            # unknown model, quota exhausted) far better than the status code.
            detail = resp.text[:600]
            if resp.status_code not in RETRY_STATUS:
                raise GenerationError(f"Gemini HTTP {resp.status_code}: {detail}")
            last = f"HTTP {resp.status_code}: {detail}"

        if attempt < attempts:
            wait = 5 * (2 ** (attempt - 1))  # 5s, 10s, 20s
            print(f"  attempt {attempt}/{attempts} failed ({last}) — retrying in {wait}s")
            time.sleep(wait)

    raise GenerationError(f"Gemini call failed after {attempts} attempts — {last}")


def _extract_text(payload: dict) -> str:
    """Pull the JSON text out of a generateContent response, explaining refusals."""
    candidates = payload.get("candidates") or []
    if not candidates:
        # No candidate at all almost always means the prompt was blocked.
        feedback = payload.get("promptFeedback", {})
        raise GenerationError(f"Gemini returned no candidates (promptFeedback={feedback}).")

    cand = candidates[0]
    parts = (cand.get("content") or {}).get("parts") or []
    text = "".join(p.get("text", "") for p in parts).strip()

    if not text:
        reason = cand.get("finishReason", "unknown")
        # MAX_TOKENS here usually means thinking ate the budget — see the header note.
        raise GenerationError(
            f"Gemini returned an empty response (finishReason={reason}). "
            "If MAX_TOKENS, raise GEMINI_MAX_OUTPUT_TOKENS or disable thinking."
        )
    return text


def _parse_rows(text: str) -> list[dict]:
    # responseSchema makes bare JSON the norm, but a fenced block is cheap to survive.
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```")[1]
        cleaned = cleaned[4:] if cleaned.lower().startswith("json") else cleaned
    try:
        rows = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise GenerationError(f"Gemini response was not valid JSON: {exc}\n{text[:400]}")
    if not isinstance(rows, list):
        raise GenerationError(f"Expected a JSON list, got {type(rows).__name__}.")
    return rows


# Soft ceilings matching the per-level budgets in summary_spec.md, with slack —
# these only warn, since a slightly long but correct paragraph is not worth failing
# the day's run over. Used to spot the prompt drifting, not to police the model.
WORD_CEILING = {"Normal": 85, "Warning": 115, "Severe": 140}


def _validate(rows: list[dict], pack: dict) -> list[dict]:
    """Every input city must come back exactly once, with real prose.

    A single call for all cities is efficient but can silently drop one, so
    coverage is checked here; a failure raises and Airflow retries the task.

    Also attaches alert_level / alert_headline from the PACK — never from the model.
    Those drive the colour coding on the Power BI page, so they have to be the
    deterministic classification computed in fetch_inputs.classify_alerts(); letting
    the model restate them would make the colour depend on its wording.
    """
    expected = {c["city_date_key"]: c["city"] for c in pack["cities"]}
    alerts = {c["city_date_key"]: (c.get("alerts") or {}) for c in pack["cities"]}
    by_key = {}

    for r in rows:
        missing = {"city_date_key", "city", "date_utc", "summary_text"} - r.keys()
        if missing:
            raise GenerationError(f"Row {r.get('city', '?')} missing fields: {sorted(missing)}")
        key = r["city_date_key"]
        if key not in expected:
            raise GenerationError(
                f"Row for {r['city']!r} has city_date_key {key!r}, which is not in "
                "the input pack (the model altered a key — it must copy them verbatim)."
            )
        if not r["summary_text"].strip():
            raise GenerationError(f"Empty summary_text for {r['city']!r}.")

        text = " ".join(r["summary_text"].split())  # flatten stray newlines
        level = alerts.get(key, {}).get("level") or "Normal"
        words = len(text.split())
        if words > WORD_CEILING.get(level, 115):
            print(f"  WARNING: {r['city']} summary is {words} words "
                  f"(level={level}, soft ceiling {WORD_CEILING.get(level)}).")

        by_key[key] = {
            "city_date_key": key,
            "city": r["city"],
            "date_utc": r["date_utc"],
            "summary_text": text,
            "alert_level": level,
            "alert_headline": alerts.get(key, {}).get("headline") or "",
        }

    absent = [expected[k] for k in expected if k not in by_key]
    if absent:
        raise GenerationError(
            f"Gemini returned {len(by_key)} of {len(expected)} cities — missing: "
            f"{', '.join(sorted(absent))}."
        )

    # Keep the input's city order so the output file reads predictably.
    return [by_key[c["city_date_key"]] for c in pack["cities"]]


def _thinking_config(model: str) -> dict | None:
    """The right thinking field for this model family, or None to omit it.

    Gemini 3+ takes thinkingLevel; 1.x/2.x take a numeric thinkingBudget. Each
    family rejects the other's field with an unhelpful 400, so pick by name and
    treat unknown//future families as Gemini 3-style (the current direction).
    """
    if re.match(r"gemini-[12][.\-]", model):
        budget = int(os.getenv("GEMINI_THINKING_BUDGET", "0"))
        return {"thinkingBudget": budget} if budget >= 0 else None

    level = os.getenv("GEMINI_THINKING_LEVEL", "LOW").strip().upper()
    return {"thinkingLevel": level} if level else None


def generate(pack: dict, model: str = DEFAULT_MODEL, timeout: int = 120) -> list[dict]:
    """Generate one summary per city in the pack. Returns step C's row list."""
    max_output = int(os.getenv("GEMINI_MAX_OUTPUT_TOKENS", "8192"))

    generation_config = {
        "temperature": 0.2,  # grounded reporting, not creative writing
        "maxOutputTokens": max_output,
        "responseMimeType": "application/json",
        "responseSchema": RESPONSE_SCHEMA,
    }
    thinking = _thinking_config(model)
    if thinking:
        generation_config["thinkingConfig"] = thinking

    body = {
        "systemInstruction": {"parts": [{"text": read_spec()}]},
        "contents": [{
            "role": "user",
            "parts": [{"text":
                       "Context pack (JSON) — write one summary per city in `cities`:\n\n"
                       + json.dumps(pack, ensure_ascii=False)}],
        }],
        "generationConfig": generation_config,
    }

    print(f"Calling {model} for {len(pack['cities'])} cities ({pack['target_date']})…")
    try:
        payload = _post(model, body, timeout)
    except GenerationError as exc:
        # A 400 with thinkingConfig present is almost always the field-name mismatch
        # above. Dropping it costs a few thinking tokens; failing costs the day's run.
        if thinking and "HTTP 400" in str(exc):
            print(f"  thinkingConfig {thinking} rejected by {model} — retrying without it.")
            generation_config.pop("thinkingConfig", None)
            payload = _post(model, body, timeout)
        else:
            raise

    usage = payload.get("usageMetadata", {})
    if usage:
        print(f"  tokens: prompt={usage.get('promptTokenCount')} "
              f"output={usage.get('candidatesTokenCount')} "
              f"total={usage.get('totalTokenCount')}")

    rows = _validate(_parse_rows(_extract_text(payload)), pack)
    print(f"  generated {len(rows)} summaries.")
    return rows


def write_rows(rows: list[dict], target_date: str):
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUTS_DIR / f"{target_date}.json"
    out.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def load_pack_file(target_date: str | None) -> dict:
    files = sorted(INPUTS_DIR.glob("*.json"))
    path = INPUTS_DIR / f"{target_date}.json" if target_date else (files[-1] if files else None)
    if path is None or not path.exists():
        raise GenerationError(f"No context pack found in {INPUTS_DIR}. Run fetch_inputs.py first.")
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    ap = argparse.ArgumentParser(description="Generate city summaries with the Gemini API.")
    ap.add_argument("--date", help="Target date YYYY-MM-DD (default: latest in mart_city_daily)")
    ap.add_argument("--model", default=DEFAULT_MODEL, help=f"Gemini model (default: {DEFAULT_MODEL})")
    ap.add_argument("--from-file", action="store_true",
                    help="Read the pack from ai/_inputs/<date>.json instead of querying Postgres")
    ap.add_argument("--dry-run", action="store_true",
                    help="Build the pack and print its size; make no API call")
    args = ap.parse_args()

    try:
        pack = load_pack_file(args.date) if args.from_file else fetch_inputs.build_pack(args.date)
        if args.dry_run:
            print(f"Dry run — pack for {pack['target_date']}: {len(pack['cities'])} cities, "
                  f"{len(json.dumps(pack))} chars. No API call made.")
            return
        rows = generate(pack, args.model)
    except (GenerationError, RuntimeError, LookupError) as exc:
        sys.exit(str(exc))

    out = write_rows(rows, pack["target_date"])
    print(f"Wrote {out}. Load it with:\n"
          f"  python ai/load_summaries.py --date {pack['target_date']} --model {args.model}")


if __name__ == "__main__":
    main()
