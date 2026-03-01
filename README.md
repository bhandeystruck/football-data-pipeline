# Football Data Engineering Pipeline (football-data.org → MinIO → Snowflake → dbt)

Production-style data engineering pipeline that ingests football match data from the football-data.org REST API, lands immutable raw JSON in MinIO (Bronze), loads raw JSON into Snowflake (Bronze), and models curated Silver/Gold analytics layers with dbt (including automated tests).

This repository is built to be replayable, idempotent, and ready for orchestration (Airflow planned next).

---

## What’s implemented

### Source → Bronze (MinIO)
- Ingest `/competitions`
- Ingest `/competitions/{code}/matches` incrementally (yesterday → next 7 days)
- Backfill `/competitions/{code}/matches` in chunks (season-to-date)
- Store each payload as immutable JSON in MinIO
- Write a companion `.manifest.json` next to each match payload with metadata (run_id, params, fetched_at_utc, record_count, etc.)

### Bronze (Snowflake)
- Store payloads in VARIANT columns:
  - `FOOTBALL_DB.BRONZE.RAW_COMPETITIONS`
  - `FOOTBALL_DB.BRONZE.RAW_MATCHES`
  - `FOOTBALL_DB.BRONZE.RAW_MANIFESTS`
- Idempotent loads via state table:
  - `FOOTBALL_DB.BRONZE.LOAD_STATE` (tracks loaded MinIO `FILE_KEY`s)

### Silver (dbt)
- `SILVER.v_matches` (view): flattens `RAW_MATCHES.PAYLOAD:matches` to one row per match
- `SILVER.matches_latest` (incremental merge): latest snapshot per `match_id` (handles match updates)
- `SILVER.teams` (incremental merge): deduped teams dimension (unique `team_id`)

### Gold (dbt)
- `GOLD.team_form_last5`: last 5 finished matches per team (W/D/L, goals, points)
- `GOLD.league_table_snapshot`: season-to-date league table (points, GD, rank)

### Data quality (dbt)
- `dbt test` with not-null/unique/accepted-values checks (all passing)

---

## Architecture

football-data.org API  
→ MinIO (Bronze object storage)  
→ Snowflake (Bronze raw tables)  
→ dbt (Silver/Gold transformations + tests)

Important note:
Snowflake cannot access `localhost` MinIO directly (Snowflake runs in the cloud). For local development, a Python loader moves data from MinIO into Snowflake.

---

## Repository Structure

- `infra/`  
  Docker compose for MinIO (Airflow will be added later)

- `ingestion/`  
  Python ingestion scripts (API → MinIO)  
  Python loaders (MinIO → Snowflake)  
  Load-state logic for idempotency

- `dbt/football_dbt/`  
  dbt project (Silver/Gold models, tests, macros)

- `docs/`, `dashboards/`  
  Reserved for diagrams/BI assets (optional)

---

## Prerequisites

- Docker Desktop
- Python 3.11+
- Snowflake account
- football-data.org API token

---

## Local Setup

### 1) Start MinIO
```bash
cd infra
docker compose up -d
```

MinIO Console:
- http://localhost:9001

Create bucket:
- `football-bronze`

---

## Configuration

### Root `.env` (NOT committed)
Create a `.env` at repository root (do not commit). Example keys:

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
- `SNOWFLAKE_ACCOUNT=...` (example: kl18145.ap-south-1.aws)
- `SNOWFLAKE_USER=FOOTBALL_PIPELINE_USER`
- `SNOWFLAKE_PASSWORD=...`
- `SNOWFLAKE_ROLE=FOOTBALL_PIPELINE_ROLE`
- `SNOWFLAKE_WAREHOUSE=FOOTBALL_WH`
- `SNOWFLAKE_DATABASE=FOOTBALL_DB`

---

## Snowflake Setup (one-time)

Create:
- Warehouse: `FOOTBALL_WH`
- Database: `FOOTBALL_DB`
- Schemas: `BRONZE`, `SILVER`, `GOLD`
- Role: `FOOTBALL_PIPELINE_ROLE`
- User: `FOOTBALL_PIPELINE_USER`

Bronze tables:
- `BRONZE.RAW_COMPETITIONS`
- `BRONZE.RAW_MATCHES`
- `BRONZE.RAW_MANIFESTS`
- `BRONZE.LOAD_STATE` (idempotent file tracking)

Note:
Local MinIO is not directly reachable by Snowflake. Python loaders move MinIO objects into Snowflake.

---

## Python Environments

This repo uses separate virtual environments to avoid dependency conflicts:

- `ingestion/.venv` → ingestion + MinIO/Snowflake loaders  
- `dbt/.venv` → dbt only

---

## Ingestion: API → MinIO (Bronze)

Activate ingestion venv:
```bash
cd ingestion
# Windows:
.\.venv\Scripts\Activate.ps1
# Mac/Linux:
source .venv/bin/activate
cd ..
```

Ingest competitions:
```bash
python -m ingestion.src.ingest_competitions
```

Ingest matches (incremental window):
```bash
python -m ingestion.src.ingest_matches_incremental
```

Backfill matches (chunked):
```bash
python -m ingestion.src.ingest_matches_backfill
```

---

## Loading: MinIO → Snowflake (Bronze)

### Idempotency via LOAD_STATE
Snowflake table `FOOTBALL_DB.BRONZE.LOAD_STATE` tracks loaded MinIO object keys. Loaders:
- list MinIO keys by prefix
- subtract keys already in LOAD_STATE
- load only new objects
- mark loaded keys in LOAD_STATE

Load backfill files (idempotent):
```bash
python -m ingestion.src.load_backfill_matches_to_snowflake
```

Load incremental files (idempotent):
```bash
python -m ingestion.src.load_incremental_matches_to_snowflake
```

---

## dbt Setup

Activate dbt venv:
```bash
cd dbt
# Windows:
.\.venv\Scripts\Activate.ps1
# Mac/Linux:
source .venv/bin/activate
cd football_dbt
```

dbt profile:
- Stored in `~/.dbt/profiles.yml`
- Uses environment variables:
  `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`

Windows env loading:
- dbt does not automatically load `.env`
- Use `set_env.ps1` in repo root to export `.env` variables into the current PowerShell process:
```bash
powershell -ExecutionPolicy Bypass -File .\set_env.ps1
```

Custom schema macro (important):
- `macros/generate_schema_name.sql` forces dbt to use exact schemas and prevents schema concatenation (e.g., `SILVER_GOLD`)

---

## dbt: Run and Test

From `dbt/football_dbt`:

Run everything:
```bash
dbt run
```

Run only Silver:
```bash
dbt run --select models/silver
```

Run only Gold:
```bash
dbt run --select models/gold
```

Run tests:
```bash
dbt test
```

---

## Outputs

Silver:
- `FOOTBALL_DB.SILVER.v_matches`
- `FOOTBALL_DB.SILVER.matches_latest`
- `FOOTBALL_DB.SILVER.teams`

Gold:
- `FOOTBALL_DB.GOLD.team_form_last5`
- `FOOTBALL_DB.GOLD.league_table_snapshot`

---

## Common one-time ownership/grant notes

If an object (view/table) was created manually earlier, dbt may fail to replace it due to ownership.
One-time fix is to either:
- transfer ownership to `FOOTBALL_PIPELINE_ROLE`, or
- drop the object and let dbt recreate it.

---

## Next step (planned): Airflow orchestration

Planned DAG tasks:
1) ingest API → MinIO
2) load incremental MinIO → Snowflake using LOAD_STATE
3) dbt run (Silver + Gold)
4) dbt test

Optional scheduled backfill job for repairs.
