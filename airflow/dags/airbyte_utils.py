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


def wait_for_sync(job_id: str, timeout: int = 2100, poll_interval: int = 30) -> None:
    """Poll Airbyte until the job completes. Raises on failure or timeout."""
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
            status = resp.json()["job"]["status"]


        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (401, 403):
                # Airbyte tokens expire in minutes — force re-auth on next _headers() call
                global _token
                _token = None
                print(f"  Token expired while polling job {job_id} — re-authenticating")
                time.sleep(poll_interval)
                continue
            print(f"  HTTP error polling job {job_id}: {e} — retrying in {poll_interval}s")
            time.sleep(poll_interval)
            continue
        except requests.exceptions.RequestException as e:
            # Transient network blip (e.g. the abctl ingress dropping a connection)
            # must NOT fail the task — a single bad poll is not a sync failure.
            # Log and poll again; only a real job failure or the timeout ends the wait.
            print(f"  Poll error for job {job_id}: {e} — retrying in {poll_interval}s")
            time.sleep(poll_interval)
            continue
        print(f"  Job {job_id} status: {status}")
        if status == "succeeded":
            return
        if status in ("failed", "cancelled", "error"):
            raise RuntimeError(f"Airbyte job {job_id} ended with status: {status}")
        time.sleep(poll_interval)
    raise TimeoutError(f"Airbyte job {job_id} did not complete within {timeout}s")
