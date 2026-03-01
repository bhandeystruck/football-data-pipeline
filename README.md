# Football Data Engineering Pipeline (football-data.org → MinIO → Snowflake → dbt)

A production-style data engineering project that ingests football match data from the football-data.org REST API, stores immutable raw payloads in MinIO (Bronze), loads raw JSON into Snowflake (Bronze), and models curated analytics layers with dbt (Silver/Gold) including data quality tests.

This repository is designed to be reproducible, testable, and incrementally extensible toward full orchestration (Airflow planned next).

---

## Architecture

Source → Storage → Warehouse → Modeling

- football-data.org (REST API)
- MinIO (S3-compatible object storage) as Bronze landing zone
- Snowflake as warehouse for Bronze/Silver/Gold
- dbt for transformations and testing

Key production characteristics:
- Raw data is immutable and replayable (Bronze JSON in MinIO)
- Loads to Snowflake are idempotent (file-key-based)
- Transformation models are version-controlled and tested (dbt)
- Incremental strategy handles mutable match records (status/score updates)

---

## Repository Structure

- `infra/`
  - Docker compose for MinIO (Airflow to be added later)
- `ingestion/`
  - API ingestion scripts (football-data.org → MinIO)
  - Loaders (MinIO → Snowflake)
  - Load-state logic to load only new files
- `dbt/football_dbt/`
  - dbt project
  - Silver models (flatten + latest snapshot + dimensions)
  - Gold marts (dashboard-ready tables)
  - dbt tests
- `dashboards/`
  - Reserved for BI artifacts / queries (optional)
- `docs/`
  - Reserved for diagrams and design docs (optional)

---

## Data Layers

### Bronze (MinIO)
Immutable raw payloads are written to MinIO, partitioned by endpoint, competition, date window, dt, and run_id.

Examples:
- `endpoint=competitions/dt=YYYY-MM-DD/run_id=<uuid>.json`
- `endpoint=matches/competition=PL/dateFrom=.../dateTo=.../dt=.../run_id=<uuid>.json`
- `endpoint=matches_backfill/competition=PL/dateFrom=.../dateTo=.../dt=.../run_id=<uuid>.json`

For match payloads, a corresponding manifest is written next to each JSON file:
- `...run_id=<uuid>.manifest.json`

Manifest fields include run_id, endpoint, competition, params, fetched_at_utc, record_count, and the exact object key.

### Bronze (Snowflake)
Raw MinIO JSON payloads are loaded into Snowflake tables using VARIANT columns:

- `FOOTBALL_DB.BRONZE.RAW_COMPETITIONS`
- `FOOTBALL_DB.BRONZE.RAW_MATCHES`
- `FOOTBALL_DB.BRONZE.RAW_MANIFESTS`

Idempotency is achieved via:
- `FOOTBALL_DB.BRONZE.LOAD_STATE` keyed by `FILE_KEY`

### Silver (dbt)
Curated, typed tables for analytics.

- `SILVER.v_matches` (view)
  - Flattens `RAW_MATCHES.PAYLOAD:matches` into one row per match
- `SILVER.matches_latest` (incremental, merge)
  - Latest snapshot per `match_id` (handles match updates over time)
- `SILVER.teams` (incremental, merge)
  - Deduped teams dimension derived from matches_latest

### Gold (dbt)
Dashboard-ready marts.

- `GOLD.team_form_last5`
  - Last 5 finished matches per team: W/D/L, goals, points
- `GOLD.league_table_snapshot`
  - Season-to-date league table: played, W/D/L, GF/GA/GD, points, rank

---

## Prerequisites

- Docker Desktop
- Python 3.11+
- Snowflake account
- football-data.org API token

---

## Configuration

### Root `.env` (not committed)
Create a local `.env` in repository root (do not commit). Example keys:

MinIO:
- `MINIO_ENDPOINT=http://localhost:9000`
- `MINIO_ACCESS_KEY=...`
- `MINIO_SECRET_KEY=...`
- `MINIO_BRONZE_BUCKET=football-bronze`
- `MINIO_REGION=us-east-1`

football-data.org:
- `FOOTBALL_DATA_API_TOKEN=...`
- `TARGET_COMPETITION_CODES=PL`

Backfill:
- `BACKFILL_COMPETITION_CODE=PL`
- `BACKFILL_START_DATE=2025-08-15`
- `BACKFILL_END_DATE=2026-02-28`
- `BACKFILL_CHUNK_DAYS=30`

Snowflake:
- `SNOWFLAKE_ACCOUNT=...`
- `SNOWFLAKE_USER=FOOTBALL_PIPELINE_USER`
- `SNOWFLAKE_PASSWORD=...`
- `SNOWFLAKE_ROLE=FOOTBALL_PIPELINE_ROLE`
- `SNOWFLAKE_WAREHOUSE=FOOTBALL_WH`
- `SNOWFLAKE_DATABASE=FOOTBALL_DB`

A `.env.example` can be included with placeholders for onboarding.

---

## Local Services

### Start MinIO
From repo root:
```bash
cd infra
docker compose up -d