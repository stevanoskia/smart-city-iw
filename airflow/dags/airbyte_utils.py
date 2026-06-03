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
        timeout=30,
    )
    resp.raise_for_status()
    _token = resp.json()["access_token"]
    return _token


def _headers() -> dict:
    return {"Authorization": f"Bearer {get_token()}"}


def trigger_sync(connection_id: str) -> str:
    """Trigger an Airbyte sync and return the job ID."""
    resp = requests.post(
        f"{AIRBYTE_URL}/api/v1/connections/sync",
        headers=_headers(),
        json={"connectionId": connection_id},
        timeout=30,
    )
    resp.raise_for_status()
    job_id = resp.json()["job"]["id"]
    print(f"  Triggered sync for connection {connection_id} → job {job_id}")
    return str(job_id)


def wait_for_sync(job_id: str, timeout: int = 3600, poll_interval: int = 30) -> None:
    """Poll Airbyte until the job completes. Raises on failure or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(
            f"{AIRBYTE_URL}/api/v1/jobs/get",
            headers=_headers(),
            params={"id": job_id},
            timeout=30,
        )
        resp.raise_for_status()
        status = resp.json()["job"]["status"]
        print(f"  Job {job_id} status: {status}")
        if status == "succeeded":
            return
        if status in ("failed", "cancelled", "error"):
            raise RuntimeError(f"Airbyte job {job_id} ended with status: {status}")
        time.sleep(poll_interval)
    raise TimeoutError(f"Airbyte job {job_id} did not complete within {timeout}s")
