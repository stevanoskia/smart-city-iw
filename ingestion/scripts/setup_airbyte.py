"""
Idempotent Airbyte setup script.

Reads ingestion/config/sources.yml and connections.yml, then creates any missing
sources, destinations, and connections via the Airbyte public API (v1).
Safe to re-run — skips resources that already exist.

After running, writes ingestion/config/connection_ids.yml with the UUID of every
connection. Airflow DAGs read this file to get the connection IDs they need.

Usage:
    python ingestion/scripts/setup_airbyte.py

Requirements:
    pip install requests pyyaml python-dotenv

Auth:
    Requires AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET in .env.
    Get them from Airbyte UI → User (bottom-left) → Applications.
"""

import os
import sys
import yaml
import requests
from pathlib import Path
from dotenv import load_dotenv

# ── Paths ────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "ingestion" / "config"
SOURCES_FILE = CONFIG_DIR / "sources.yml"
CONNECTIONS_FILE = CONFIG_DIR / "connections.yml"
CONNECTION_IDS_FILE = CONFIG_DIR / "connection_ids.yml"

# ── Load environment ──────────────────────────────────────────────────────────

load_dotenv(ROOT / ".env")

AIRBYTE_URL          = os.getenv("AIRBYTE_URL", "http://localhost:8000").rstrip("/")
AIRBYTE_CLIENT_ID    = os.getenv("AIRBYTE_CLIENT_ID")
AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")
AIRBYTE_WORKSPACE_ID = os.getenv("AIRBYTE_WORKSPACE_ID")

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
TOMTOM_API_KEY      = os.getenv("TOMTOM_API_KEY")
PG_HOST = os.getenv("AIRBYTE_PG_HOST")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "smart_city")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD")

# ── Auth ──────────────────────────────────────────────────────────────────────

_token: str | None = None

def get_token() -> str:
    global _token
    if _token:
        return _token
    if not AIRBYTE_CLIENT_ID or not AIRBYTE_CLIENT_SECRET:
        raise RuntimeError(
            "AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET must be set in .env.\n"
            "Get them from Airbyte UI → User → Applications."
        )
    resp = requests.post(
        f"{AIRBYTE_URL}/api/v1/applications/token",
        json={
            "client_id": AIRBYTE_CLIENT_ID,
            "client_secret": AIRBYTE_CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(
            f"Failed to obtain Airbyte token: {resp.status_code} {resp.text[:300]}"
        )
    _token = resp.json()["access_token"]
    print("  ✓ Token obtained")
    return _token

# ── API helpers (new public API at /v1/) ─────────────────────────────────────

def api(method: str, path: str, **kwargs):
    """Call the Airbyte API with bearer token auth."""
    url = f"{AIRBYTE_URL}/api/v1/{path.lstrip('/')}"
    headers = {"Authorization": f"Bearer {get_token()}"}
    resp = requests.request(
        method, url, headers=headers,
        json=kwargs.get("json"), params=kwargs.get("params"),
        timeout=30,
    )
    if not resp.ok:
        print(f"  ERROR {resp.status_code} {method} {path}: {resp.text[:300]}")
        resp.raise_for_status()
    if not resp.text.strip():
        return {}
    return resp.json()


def get_workspace_id() -> str:
    if AIRBYTE_WORKSPACE_ID:
        return AIRBYTE_WORKSPACE_ID
    data = api("POST", "workspaces/list")
    workspaces = data.get("workspaces", [])
    if not workspaces:
        raise RuntimeError(
            "No workspaces found. Set AIRBYTE_WORKSPACE_ID in .env — "
            "find it in the Airbyte UI URL: localhost:8000/workspaces/<uuid>/..."
        )
    return workspaces[0]["workspaceId"]


def list_sources(workspace_id: str) -> dict:
    data = api("POST", "sources/list", json={"workspaceId": workspace_id})
    return {s["name"]: s for s in data.get("sources", [])}


def list_destinations(workspace_id: str) -> dict:
    data = api("POST", "destinations/list", json={"workspaceId": workspace_id})
    return {d["name"]: d for d in data.get("destinations", [])}


def list_connections(workspace_id: str) -> dict:
    data = api("POST", "connections/list", json={"workspaceId": workspace_id})
    return {c["sourceId"]: c for c in data.get("connections", [])}


def find_custom_source_definition(workspace_id: str, name: str) -> str:
    data = api(
        "POST", "source_definitions/list_for_workspace",
        json={"workspaceId": workspace_id},
    )
    for defn in data.get("sourceDefinitions", []):
        if defn.get("name") == name:
            return defn["sourceDefinitionId"]
    raise RuntimeError(
        f"Connector '{name}' not found. "
        "Make sure it is published in the Airbyte Connector Builder UI."
    )


def find_postgres_destination_definition() -> str:
    data = api("GET", "destination_definitions/list")
    for defn in data.get("destinationDefinitions", []):
        if "postgres" in defn.get("name", "").lower():
            return defn["destinationDefinitionId"]
    raise RuntimeError("PostgreSQL destination definition not found")


def ensure_destination(workspace_id: str, cfg: dict, existing: dict) -> str:
    name = cfg["destination"]["name"]
    if name in existing:
        print(f"  ✓ Destination '{name}' already exists — skipping")
        return existing[name]["destinationId"]

    if not PG_HOST:
        raise RuntimeError(
            "AIRBYTE_PG_HOST is not set in .env. "
            "Set it to your machine's LAN IP (e.g. 10.2.12.150), not localhost."
        )

    defn_id = find_postgres_destination_definition()
    result = api(
        "POST", "destinations/create",
        json={
            "workspaceId": workspace_id,
            "name": name,
            "destinationDefinitionId": defn_id,
            "connectionConfiguration": {
                "host": PG_HOST,
                "port": PG_PORT,
                "database": PG_DB,
                "username": PG_USER,
                "password": PG_PASS,
                "schema": cfg["destination"]["schema"],
                "ssl": cfg["destination"]["ssl"],
            },
        },
    )
    print(f"  + Destination '{name}' created")
    return result["destinationId"]


def ensure_source(workspace_id: str, defn_id: str, source_name: str,
                  source_cfg: dict, existing: dict) -> str:
    if source_name in existing:
        print(f"  ✓ Source '{source_name}' already exists — skipping")
        return existing[source_name]["sourceId"]

    result = api(
        "POST", "sources/create",
        json={
            "workspaceId": workspace_id,
            "name": source_name,
            "sourceDefinitionId": defn_id,
            "connectionConfiguration": source_cfg,
        },
    )
    print(f"  + Source '{source_name}' created")
    return result["sourceId"]


def discover_catalog(source_id: str) -> dict:
    """Discover the source schema and return a syncCatalog ready for connection create."""
    print(f"    Discovering schema for source {source_id}...")
    data = api("POST", "sources/discover_schema", json={"sourceId": source_id})
    catalog = data.get("catalog", {})
    streams = catalog.get("streams", [])
    return {
        "streams": [
            {
                "stream": s["stream"],
                "config": {
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "append",
                    "selected": True,
                },
            }
            for s in streams
        ]
    }


def set_connection_manual(connection_id: str, conn_name: str) -> None:
    """Switch an existing connection to manual schedule (Airflow owns triggering)."""
    result = api(
        "POST", "connections/update",
        json={
            "connectionId": connection_id,
            "scheduleType": "manual",
        },
    )
    schedule = result.get("scheduleType", "unknown")
    print(f"  ✓ Connection '{conn_name}' schedule set to: {schedule}")


def ensure_connection(workspace_id: str, source_id: str, destination_id: str,
                      conn_name: str, streams: list, sync_cfg: dict,
                      existing_by_source: dict) -> str:
    if source_id in existing_by_source:
        conn = existing_by_source[source_id]
        conn_id = conn["connectionId"]
        print(f"  ✓ Connection '{conn_name}' already exists — setting to manual schedule")
        set_connection_manual(conn_id, conn_name)
        return conn_id

    sync_catalog = discover_catalog(source_id)
    result = api(
        "POST", "connections/create",
        json={
            "sourceId": source_id,
            "destinationId": destination_id,
            "name": conn_name,
            "status": "active",
            "scheduleType": "manual",   # Airflow triggers syncs — no internal schedule
            "syncCatalog": sync_catalog,
            "namespaceDefinition": "customformat",
            "namespaceFormat": "airbyte_raw",
        },
    )
    print(f"  + Connection '{conn_name}' created")
    return result["connectionId"]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    sources_cfg = yaml.safe_load(SOURCES_FILE.read_text())
    conn_cfg    = yaml.safe_load(CONNECTIONS_FILE.read_text())
    sync        = conn_cfg["sync"]

    print("Connecting to Airbyte at", AIRBYTE_URL)
    workspace_id = get_workspace_id()
    print(f"Workspace: {workspace_id}\n")

    existing_sources      = list_sources(workspace_id)
    existing_destinations = list_destinations(workspace_id)
    existing_connections  = list_connections(workspace_id)

    # Destination
    print("── Destination ─────────────────────────────")
    destination_id = ensure_destination(workspace_id, conn_cfg, existing_destinations)

    connection_ids = {}

    # OpenWeather sources
    print("\n── OpenWeather sources ──────────────────────")
    ow_cfg    = sources_cfg["openweather"]
    ow_defn_id = find_custom_source_definition(workspace_id, ow_cfg["connector_name"])

    for city in ow_cfg["cities"]:
        name = f"openweather_{city['city'].lower()}"
        source_cfg = {
            "city":  city["city"],
            "lat":   city["lat"],
            "lon":   city["lon"],
            "appid": OPENWEATHER_API_KEY,
        }
        source_id = ensure_source(workspace_id, ow_defn_id, name, source_cfg, existing_sources)
        conn_id   = ensure_connection(
            workspace_id, source_id, destination_id,
            name, ow_cfg["streams"], sync, existing_connections,
        )
        connection_ids[name] = conn_id

    # TomTom sources
    print("\n── TomTom sources ───────────────────────────")
    tt_cfg    = sources_cfg["tomtom"]
    tt_defn_id = find_custom_source_definition(workspace_id, tt_cfg["connector_name"])

    for city in tt_cfg["cities"]:
        name = f"tomtom_{city['city'].lower()}"
        source_cfg = {
            "city":    city["city"],
            "lat":     city["lat"],
            "lon":     city["lon"],
            "min_lat": city["min_lat"],
            "min_lon": city["min_lon"],
            "max_lat": city["max_lat"],
            "max_lon": city["max_lon"],
            "api_key": TOMTOM_API_KEY,
        }
        source_id = ensure_source(workspace_id, tt_defn_id, name, source_cfg, existing_sources)
        conn_id   = ensure_connection(
            workspace_id, source_id, destination_id,
            name, tt_cfg["streams"], sync, existing_connections,
        )
        connection_ids[name] = conn_id

    # Write connection IDs for Airflow
    print("\n── Connection IDs (written to connection_ids.yml) ──")
    for name, cid in connection_ids.items():
        print(f"  {name}: {cid}")

    CONNECTION_IDS_FILE.write_text(
        "# Auto-generated by setup_airbyte.py — do not edit manually\n"
        + yaml.dump(connection_ids, default_flow_style=False)
    )

    print("\nDone.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFailed: {e}", file=sys.stderr)
        sys.exit(1)
