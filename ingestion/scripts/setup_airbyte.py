"""
Idempotent, config-driven Airbyte setup script.

Reads the sources / streams / cities from the metadata `config` schema in Postgres
(config.sources, config.streams, config.source_locations — see config/schema.sql),
then creates any missing sources, destinations, and connections via the Airbyte
public API (v1). Safe to re-run — updates existing resources in place. Adding a city
or a source is a plain INSERT into config.* (no YAML edit); re-run this to apply it.

After running, writes ingestion/config/connection_ids.yml with the UUID of every
connection. Airflow DAGs read this file to get the connection IDs they need.

Two entrypoints:
    main()       host — manages the destination (pushes this machine's LAN IP).
                 Run this after a network switch.  `python ingestion/scripts/setup_airbyte.py`
    reconcile()  container-safe (the Airflow `reconcile_airbyte` task) — does NOT
                 touch the destination (LAN-IP detection is a host-only concern);
                 reuses the existing destination and just applies config.* changes.

Requirements:
    pip install requests pyyaml python-dotenv psycopg2-binary

Auth:
    Requires AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET in .env.
    Get them from Airbyte UI → User (bottom-left) → Applications.
"""

import os
import sys
import socket
import yaml
import psycopg2
import requests
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # not installed in the Airflow container — env is already populated there
    def load_dotenv(*_args, **_kwargs):
        return False

# Windows consoles default to cp1252; force UTF-8 so status glyphs (✓ ~ +) don't crash.
sys.stdout.reconfigure(encoding="utf-8")

# ── Paths ────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "ingestion" / "config"
# Overridable via env so the Airflow container (whose mount paths differ from the repo
# layout) can point at the mounted config dir (/opt/airflow/ingestion_config).
CONNECTION_IDS_FILE = Path(
    os.getenv("CONNECTION_IDS_FILE", str(CONFIG_DIR / "connection_ids.yml"))
)

# The single Postgres destination (was ingestion/config/connections.yml). Fixed and
# not per-source, so it stays a constant here rather than a config table. The host
# resolves + pushes this machine's LAN IP into it (Airbyte pods can't reach localhost).
DESTINATION = {"name": "smart_city_postgres", "schema": "staging", "ssl": False}

# ── Load environment ──────────────────────────────────────────────────────────

load_dotenv(ROOT / ".env")

AIRBYTE_URL          = os.getenv("AIRBYTE_URL", "http://localhost:8000").rstrip("/")
AIRBYTE_CLIENT_ID    = os.getenv("AIRBYTE_CLIENT_ID")
AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")
AIRBYTE_WORKSPACE_ID = os.getenv("AIRBYTE_WORKSPACE_ID")

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
TOMTOM_API_KEY      = os.getenv("TOMTOM_API_KEY")
PG_HOST_SETTING = (os.getenv("AIRBYTE_PG_HOST") or "").strip()
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "smart_city")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD")

# ── Postgres host resolution ──────────────────────────────────────────────────
# Airbyte's sync pods run inside Kind and cannot reach this machine via localhost, so
# the destination must hold this machine's LAN IP. That IP is assigned by whatever
# network you're on (office vs home), and Airbyte stores it *literally* — so switching
# networks silently breaks every sync until the destination is re-pointed. Detecting it
# here means re-running this script is all a network switch costs.


def detect_lan_ip() -> str:
    """LAN IP of the interface holding the default route.

    Connecting a UDP socket sends no packet — it only makes the OS choose the outbound
    interface, whose address is the one Kind pods must dial. Selecting by default route
    is what keeps us off the WSL/Docker adapter (172.28.x), which pods can't use.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]


def resolve_pg_host() -> str:
    """Explicit AIRBYTE_PG_HOST wins; 'auto' or unset auto-detects the LAN IP."""
    if PG_HOST_SETTING and PG_HOST_SETTING.lower() != "auto":
        print(f"  Postgres host: {PG_HOST_SETTING} (pinned via AIRBYTE_PG_HOST)")
        return PG_HOST_SETTING

    try:
        ip = detect_lan_ip()
    except OSError as e:
        raise RuntimeError(
            f"Could not auto-detect this machine's LAN IP ({e}). "
            "Set AIRBYTE_PG_HOST in .env to an explicit IP address."
        ) from e

    if ip.startswith("127."):
        raise RuntimeError(
            f"Auto-detected a loopback address ({ip}), which Airbyte's pods cannot reach. "
            "Check you're connected to a network, or set AIRBYTE_PG_HOST explicitly."
        )
    print(f"  Postgres host: {ip} (auto-detected LAN IP)")
    return ip

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


def find_existing_destination(existing: dict, name: str) -> str:
    """Return an existing destination's id, or fail with guidance (container path)."""
    if name in existing:
        return existing[name]["destinationId"]
    raise RuntimeError(
        f"Destination '{name}' does not exist yet. Run setup_airbyte.py on the HOST first — "
        "the host manages the destination (it must hold this machine's LAN IP, which cannot "
        "be detected from inside the Airflow container)."
    )


def ensure_destination(workspace_id: str, dest: dict, existing: dict) -> str:
    name = dest["name"]
    pg_host = resolve_pg_host()
    dest_cfg = {
        "host": pg_host,
        "port": PG_PORT,
        "database": PG_DB,
        "username": PG_USER,
        "password": PG_PASS,
        "schema": dest["schema"],
        "ssl": dest["ssl"],
    }

    if name in existing:
        destination_id = existing[name]["destinationId"]
        # Push the latest config so a changed LAN IP (new network) updates the existing
        # destination instead of being skipped. Airbyte stores the host literally, so
        # this is the only thing that re-points the sync pods after a network switch.
        api(
            "POST", "destinations/update",
            json={
                "destinationId": destination_id,
                "name": name,
                "connectionConfiguration": dest_cfg,
            },
        )
        print(f"  ~ Destination '{name}' already exists — config updated (host={pg_host})")
        return destination_id

    defn_id = find_postgres_destination_definition()
    result = api(
        "POST", "destinations/create",
        json={
            "workspaceId": workspace_id,
            "name": name,
            "destinationDefinitionId": defn_id,
            "connectionConfiguration": dest_cfg,
        },
    )
    print(f"  + Destination '{name}' created (host={pg_host})")
    return result["destinationId"]


def ensure_source(workspace_id: str, defn_id: str, source_name: str,
                  source_cfg: dict, existing: dict) -> str:
    if source_name in existing:
        source_id = existing[source_name]["sourceId"]
        # Push the latest config so edits to sources.yml (e.g. new cities in the
        # `locations` list) update the existing source instead of being skipped.
        api(
            "POST", "sources/update",
            json={
                "sourceId": source_id,
                "name": source_name,
                "connectionConfiguration": source_cfg,
            },
        )
        print(f"  ~ Source '{source_name}' already exists — config updated")
        return source_id

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
            "namespaceFormat": "staging",
        },
    )
    print(f"  + Connection '{conn_name}' created")
    return result["connectionId"]


# ── Config from the DB (single source of truth) ──────────────────────────────

def pg_connect():
    """Connect to the config DB. Prefer SMART_CITY_PG_* (set in the Airflow container,
    where POSTGRES_HOST=localhost would be wrong); fall back to POSTGRES_* on the host."""
    return psycopg2.connect(
        host=os.getenv("SMART_CITY_PG_HOST") or os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("SMART_CITY_PG_PORT") or os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("SMART_CITY_PG_DB") or os.getenv("POSTGRES_DB", "smart_city"),
        user=os.getenv("SMART_CITY_PG_USER") or os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("SMART_CITY_PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD"),
    )


def read_config_from_db() -> dict:
    """Build the per-source config (connector, streams, cities) from config.* — the
    same shape the script used to read from sources.yml. Only active rows."""
    conn = pg_connect()
    try:
        cfg: dict = {}
        with conn.cursor() as cur:
            cur.execute(
                "select source_id, source_name, connector_name, api_key_env, api_key_field "
                "from config.sources where is_active order by source_name"
            )
            for source_id, source_name, connector_name, api_key_env, api_key_field in cur.fetchall():
                with conn.cursor() as c2:
                    c2.execute(
                        """
                        select l.city, l.latitude, l.longitude,
                               sl.min_lat, sl.min_lon, sl.max_lat, sl.max_lon
                        from config.source_locations sl
                        join config.locations l on l.location_id = sl.location_id
                        where sl.source_id = %s and sl.is_active and l.is_active
                        order by l.city
                        """,
                        (source_id,),
                    )
                    locations = []
                    for city, lat, lon, mnla, mnlo, mxla, mxlo in c2.fetchall():
                        loc = {"city": city, "lat": float(lat), "lon": float(lon)}
                        if mnla is not None:  # bbox present (TomTom); omit for weather
                            loc.update(min_lat=float(mnla), min_lon=float(mnlo),
                                       max_lat=float(mxla), max_lon=float(mxlo))
                        locations.append(loc)
                    c2.execute(
                        "select stream_name from config.streams "
                        "where source_id = %s and is_active order by stream_id",
                        (source_id,),
                    )
                    streams = [r[0] for r in c2.fetchall()]
                cfg[source_name] = {
                    "connector_name": connector_name,
                    "api_key_env": api_key_env,
                    "api_key_field": api_key_field,
                    "streams": streams,
                    "locations": locations,
                }
        return cfg
    finally:
        conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def build_sources_and_connections(workspace_id, destination_id, sources_cfg,
                                  existing_sources, existing_connections) -> dict:
    """One source + one connection per active API, partition-routed over its cities."""
    connection_ids = {}
    for source_name, cfg in sources_cfg.items():
        conn_name = f"{source_name}_all"
        print(f"\n── {source_name} source ───────────────────────")
        defn_id = find_custom_source_definition(workspace_id, cfg["connector_name"])
        api_key = os.getenv(cfg["api_key_env"] or "")
        if not api_key:
            raise RuntimeError(
                f"API key env var '{cfg['api_key_env']}' is not set for source '{source_name}'."
            )
        source_cfg = {cfg["api_key_field"]: api_key, "locations": cfg["locations"]}
        source_id = ensure_source(
            workspace_id, defn_id, conn_name, source_cfg, existing_sources
        )
        connection_ids[conn_name] = ensure_connection(
            workspace_id, source_id, destination_id,
            conn_name, cfg["streams"], None, existing_connections,
        )
    return connection_ids


def write_connection_ids(connection_ids: dict) -> None:
    print("\n── Connection IDs (written to connection_ids.yml) ──")
    for name, cid in connection_ids.items():
        print(f"  {name}: {cid}")
    CONNECTION_IDS_FILE.write_text(
        "# Auto-generated by setup_airbyte.py - do not edit manually\n"
        + yaml.dump(connection_ids, default_flow_style=False),
        encoding="utf-8",
    )


def _run(manage_destination: bool) -> dict:
    print("Connecting to Airbyte at", AIRBYTE_URL)
    workspace_id = get_workspace_id()
    print(f"Workspace: {workspace_id}\n")

    existing_sources      = list_sources(workspace_id)
    existing_destinations = list_destinations(workspace_id)
    existing_connections  = list_connections(workspace_id)

    print("── Destination ─────────────────────────────")
    if manage_destination:
        destination_id = ensure_destination(workspace_id, DESTINATION, existing_destinations)
    else:
        destination_id = find_existing_destination(existing_destinations, DESTINATION["name"])
        print(f"  ~ Reusing existing destination '{DESTINATION['name']}' (host-only manages it)")

    sources_cfg = read_config_from_db()
    connection_ids = build_sources_and_connections(
        workspace_id, destination_id, sources_cfg, existing_sources, existing_connections
    )
    write_connection_ids(connection_ids)
    print("\nDone.")
    return connection_ids


def main() -> dict:
    """Host entrypoint — manages the destination (pushes this machine's LAN IP)."""
    return _run(manage_destination=True)


def reconcile() -> dict:
    """Container-safe entrypoint (Airflow reconcile task) — applies config.* changes
    to Airbyte but does NOT touch the destination (LAN-IP detection is host-only)."""
    return _run(manage_destination=False)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFailed: {e}", file=sys.stderr)
        sys.exit(1)
