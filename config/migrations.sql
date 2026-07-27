-- ============================================================================
-- config/migrations.sql — idempotent retrofits for pre-existing databases
-- ============================================================================
-- schema.sql defines the CURRENT state of the config schema (every column and
-- constraint lives in its CREATE TABLE). A `create table if not exists` does NOT
-- alter an already-existing table, so a database created against an OLDER schema.sql
-- won't gain columns/constraints that were added later. This file brings such a DB
-- up to the current state.
--
-- Every statement is idempotent (add ... if not exists / drop-then-add), so this is
-- a no-op on a fresh DB just created from the current schema.sql — safe to always run.
--
-- Run order (see config/README.md):  schema.sql  →  migrations.sql  →  seed_config.py
--
--   psql "host=localhost dbname=smart_city user=postgres" -f config/migrations.sql
-- ============================================================================

-- config.sources.api_key_field — connector config key for the API key ('appid'/'api_key').
-- Added after the table originally shipped; now part of the CREATE TABLE in schema.sql.
alter table config.sources add column if not exists api_key_field text;

-- config.validation_runs failure-triage columns — now part of the CREATE TABLE in schema.sql.
alter table config.validation_runs add column if not exists resolved      boolean not null default false;
alter table config.validation_runs add column if not exists resolved_at   timestamptz;
alter table config.validation_runs add column if not exists resolved_note text;

-- config.field_mappings guard: a required field may not be deactivated (would drop the
-- column from the generated staging SELECT and break dbt_intermediate). Inline in the
-- CREATE TABLE in schema.sql for fresh DBs; drop-then-add retrofits pre-existing ones.
alter table config.field_mappings drop constraint if exists field_mappings_required_active_chk;
alter table config.field_mappings add  constraint field_mappings_required_active_chk
    check (not (is_required and not is_active));
