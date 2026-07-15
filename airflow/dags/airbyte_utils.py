"""
Airbyte OAuth helper for Airflow DAGs.

Triggers syncs and polls for completion using the Airbyte API with
bearer token authentication (client_id / client_secret OAuth flow).

Required env vars (set in docker-compose.yml):
    AIRBYTE_URL           http://host.docker.internal:8000
    AIRBYTE_CLIENT_ID     from Airbyte UI → User → Applications
    AIRBYTE_CLIENT_SECRET from Airbyte UI → User → Applications
"""

import os
import time
import requests

AIRBYTE_URL = os.environ.get("AIRBYTE_URL", "http://host.docker.internal:8000").rstrip("/")
AIRBYTE_CLIENT_ID = os.environ.get("AIRBYTE_CLIENT_ID")
AIRBYTE_CLIENT_SECRET = os.environ.get("AIRBYTE_CLIENT_SECRET")

_token: str | None = None


def get_token() -> str:
    global _token
    if _token:
        return _token
    resp = requests.post(
        f"{AIRBYTE_URL}/api/v1/applications/token",
        json={
            "client_id": AIRBYTE_CLIENT_ID,
            "client_secret": AIRBYTE_CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        headers={"Connection": "close"},
        timeout=30,
    )
    resp.raise_for_status()
    _token = resp.json()["access_token"]
    return _token


def _headers() -> dict:
    # Connection: close → open a fresh connection per request. Avoids the keep-alive
    # race where the abctl/Kind ingress reaps an idle pooled connection between polls,
    # which surfaces as ConnectionError/RemoteDisconnected on the next request.
    return {"Authorization": f"Bearer {get_token()}", "Connection": "close"}


def trigger_sync(connection_id: str) -> str:
    """Trigger an Airbyte sync and return the job ID.
    If a sync is already running (409), return the existing running job ID."""
    resp = requests.post(
        f"{AIRBYTE_URL}/api/v1/connections/sync",
        headers=_headers(),
        json={"connectionId": connection_id},
        timeout=30,
    )
    if resp.status_code == 409:
        # Sync already running — find the active job and return it
        print(f"  Sync already running for {connection_id}, finding active job...")
        # /api/v1/jobs/list is a Config-API endpoint — POST with a JSON body.
        jobs = requests.post(
            f"{AIRBYTE_URL}/api/v1/jobs/list",
            headers=_headers(),
            json={"configId": connection_id, "configTypes": ["sync"]},
            timeout=30,
        )
        jobs.raise_for_status()
        job_list = jobs.json().get("jobs", [])
        active = [
            j for j in job_list
            if j["job"]["status"] in ("running", "pending", "incomplete")
        ]
        if active:
            job_id = str(active[0]["job"]["id"])
            print(f"  Attached to existing job {job_id}")
            return job_id
        # No running job found — may have just finished, return sentinel
        print(f"  No running job found for {connection_id}, skipping wait")
        return "skip"
    resp.raise_for_status()
    job_id = resp.json()["job"]["id"]
    print(f"  Triggered sync for connection {connection_id} → job {job_id}")
    return str(job_id)


# ── Failure diagnosis ─────────────────────────────────────────────────────────
# The jobs/get payload already carries why a sync died — we used to throw it away and
# raise a bare "ended with status: failed", which couldn't distinguish a network problem
# from a bad API key. Shape (confirmed against a real failed job):
#
#   payload["attempts"][]              ← top-level, NOT nested under "job"
#     └── ["attempt"]["failureSummary"]["failures"][]
#           failureOrigin    source | destination | replication | normalization
#           failureType      config_error | system_error | transient_error
#           externalMessage  user-facing summary
#           internalMessage  the diagnostic one
#           stacktrace       hundreds of lines of Java — task log only, never the email

# Substring → plain-English cause, matched against the origin/type/messages of each
# failure. First match wins; an unmatched failure still shows its raw message, so this
# is a convenience layer and never hides detail.
_FAILURE_HINTS: list[tuple[str, str]] = [
    (
        "connection is not available",
        "Airbyte could not reach Postgres. If you switched networks, this machine's LAN IP "
        "moved — re-run ingestion/scripts/setup_airbyte.py to re-point the destination.",
    ),
    (
        "connection refused",
        "Postgres refused the connection — check it is running and reachable on the LAN IP.",
    ),
    (
        "no pg_hba.conf entry",
        "Postgres rejected this subnet. pg_hba.conf should carry a 'samenet' rule; verify "
        "with: SELECT type, address, auth_method FROM pg_hba_file_rules;",
    ),
    (
        "password authentication failed",
        "Wrong Postgres password in the Airbyte destination — check POSTGRES_PASSWORD "
        "in .env and re-run ingestion/scripts/setup_airbyte.py.",
    ),
    (
        "invalid api key",
        "The API rejected the key — check OPENWEATHER_API_KEY / TOMTOM_API_KEY in .env.",
    ),
    (
        "invalid appid",
        "OpenWeather rejected the key — check OPENWEATHER_API_KEY in .env.",
    ),
    (
        "too many requests",
        "API rate limit hit — usually clears on the next run.",
    ),
]


def _diagnose(failure_type: str, text: str) -> str | None:
    lowered = text.lower()
    for needle, hint in _FAILURE_HINTS:
        if needle in lowered:
            return hint
    if failure_type == "transient_error":
        return "Transient Airbyte error — the task's retries usually clear this."
    return None


def _last_failures(payload: dict) -> list[dict]:
    """Failure records from the most recent attempt that has any.

    Airbyte can retry internally and produce several attempts; the last one holds the
    definitive cause, so earlier attempts would only add noise.
    """
    for entry in reversed(payload.get("attempts", [])):
        summary = (entry.get("attempt") or {}).get("failureSummary") or {}
        if summary.get("failures"):
            return summary["failures"]
    return []


def _describe_failures(failures: list[dict]) -> str:
    """One readable block per failure: origin/type, message, and a hint where we have one."""
    lines: list[str] = []
    for f in failures:
        origin   = f.get("failureOrigin") or "unknown"
        ftype    = f.get("failureType") or "unknown"
        external = (f.get("externalMessage") or "").strip()
        internal = (f.get("internalMessage") or "").strip()

        lines.append(f"[{origin}/{ftype}] {external or internal or 'no message'}")
        if internal and internal != external:
            lines.append(f"    -> {internal}")
        hint = _diagnose(ftype, f"{origin} {ftype} {external} {internal}")
        if hint:
            lines.append(f"    hint: {hint}")
    return "\n".join(lines)


def wait_for_sync(job_id: str, timeout: int = 2100, poll_interval: int = 30) -> None:
    """Poll Airbyte until the job completes. Raises on failure or timeout.

    Default timeout (2100s / 35 min) is kept just under the wait task's
    execution_timeout (40 min) so this function's own TimeoutError — which names
    the job_id — surfaces before Airflow's generic 'task timed out' kill.
    """
    global _token
    if job_id == "skip":
        print("  Sync already completed before we attached — skipping wait")
        return
    deadline = time.time() + timeout
    while time.time() < deadline:
        # /api/v1/jobs/get is a Config-API endpoint — POST with a JSON body.
        try:
            resp = requests.post(
                f"{AIRBYTE_URL}/api/v1/jobs/get",
                headers=_headers(),
                json={"id": int(job_id)},
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
            status = payload["job"]["status"]
        except requests.exceptions.RequestException as e:
            # A single bad poll must NOT fail the task — log and poll again; only a
            # real job failure or the timeout ends the wait.
            #   • 401/403: Airbyte OAuth tokens are short-lived (~minutes). A long
            #     sync outlives the token cached at the start of this wait, so mid-
            #     poll we get 401. HTTPError subclasses RequestException, so it lands
            #     here too — drop the cached token so the next _headers() re-auths.
            #     (Without this, we'd retry the same dead token forever until the
            #     task's execution_timeout kills it — a 401 disguised as a slow sync.)
            #   • everything else (network blip, transient 5xx): plain retry, as before.
            resp_err = getattr(e, "response", None)
            if resp_err is not None and resp_err.status_code in (401, 403):
                _token = None
                print(f"  Token expired for job {job_id} — re-authenticating, retrying in {poll_interval}s")
            else:
                print(f"  Poll error for job {job_id}: {e} — retrying in {poll_interval}s")
            time.sleep(poll_interval)
            continue
        print(f"  Job {job_id} status: {status}")
        if status == "succeeded":
            return
        if status in ("failed", "cancelled", "error"):
            failures = _last_failures(payload)
            # Stacktraces go to the task log only — they'd bury the actual message in
            # the alert email. The task log is where you go once the summary isn't enough.
            for f in failures:
                if f.get("stacktrace"):
                    print(
                        f"  --- stacktrace ({f.get('failureOrigin', 'unknown')}) ---\n"
                        f"{f['stacktrace']}"
                    )
            detail = _describe_failures(failures)
            message = f"Airbyte job {job_id} ended with status: {status}"
            if detail:
                message = f"{message}\n{detail}"
            raise RuntimeError(message)
        time.sleep(poll_interval)
    raise TimeoutError(f"Airbyte job {job_id} did not complete within {timeout}s")
