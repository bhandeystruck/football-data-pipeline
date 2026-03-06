Football Data Engineering Pipeline (football-data.org → MinIO → Snowflake → dbt → Airflow)

Production-style data engineering pipeline that ingests football match data from the football-data.org REST API, lands immutable raw JSON in MinIO (Bronze), loads raw JSON into Snowflake (Bronze), models curated Silver/Gold analytics layers with dbt (including tests), and orchestrates the full workflow on a schedule using Airflow. Basic run-metrics observability is stored in Snowflake.


WHAT’S IMPLEMENTED

1) Source → Bronze (MinIO)
- Ingest /competitions
- Ingest /competitions/{code}/matches incrementally (rolling window)
- Backfill /competitions/{code}/matches in chunks (season-to-date range)
- Store each payload as immutable raw JSON in MinIO
- Write a companion .manifest.json next to each match payload with metadata:
  run_id, params, fetched_at_utc, record_count, object key, etc.

MinIO key patterns (examples)
- endpoint=competitions/dt=YYYY-MM-DD/run_id=<uuid>.json
- endpoint=matches/competition=PL/dateFrom=YYYY-MM-DD/dateTo=YYYY-MM-DD/dt=YYYY-MM-DD/run_id=<uuid>.json
- endpoint=matches_backfill/competition=PL/dateFrom=YYYY-MM-DD/dateTo=YYYY-MM-DD/dt=YYYY-MM-DD/run_id=<uuid>.json
- corresponding manifest: same key with .manifest.json

---------------------------------------------------------------------

2) Bronze (Snowflake)
Raw payloads are stored as VARIANT in Snowflake Bronze tables:

- FOOTBALL_DB.BRONZE.RAW_COMPETITIONS
- FOOTBALL_DB.BRONZE.RAW_MATCHES
- FOOTBALL_DB.BRONZE.RAW_MANIFESTS

Idempotent loading is enforced using:

- FOOTBALL_DB.BRONZE.LOAD_STATE  (tracks loaded FILE_KEY)

Idempotent loaders:
- load_incremental_matches_to_snowflake: loads only new endpoint=matches/... objects
- load_backfill_matches_to_snowflake: loads only new endpoint=matches_backfill/... objects

---------------------------------------------------------------------

3) Silver (dbt)
Curated, typed analytics-ready models:

- SILVER.v_matches (view)
  Flattens RAW_MATCHES.PAYLOAD:matches to one row per match.

- SILVER.matches_latest (incremental merge)
  Maintains the latest snapshot per match_id to capture match updates (status/score changes).

- SILVER.teams (incremental merge)
  Deduped teams dimension derived from matches_latest.

---------------------------------------------------------------------

4) Gold (dbt)
Dashboard-ready marts:

- GOLD.team_form_last5
  Last 5 finished matches per team (W/D/L, goals for/against, points).

- GOLD.league_table_snapshot
  Season-to-date league table (rank, points, GD, as_of_utc).

---------------------------------------------------------------------

5) Airflow Orchestration (Docker, LocalExecutor, Postgres metadata DB)
Airflow runs in Docker using LocalExecutor and Postgres for Airflow metadata.

Daily DAG: football_daily_pipeline (scheduled at 02:00 Nepal time)
1) ingest_matches_incremental_to_minio
2) load_incremental_minio_to_snowflake (idempotent via LOAD_STATE)
3) dbt run
4) dbt test
5) write_run_metrics_to_snowflake

Manual DAG: football_backfill_manual (trigger only, no schedule)
1) ingest_matches_backfill_to_minio
2) load_backfill_minio_to_snowflake
3) dbt run
4) dbt test

Custom Airflow image:
- infra/airflow/Dockerfile builds a custom airflow image with required packages installed:
  boto3, python-dotenv, snowflake-connector-python, dbt-core, dbt-snowflake (versions pinned)

---------------------------------------------------------------------

6) Basic Observability (Snowflake)
A final Airflow task writes run metrics into:

- FOOTBALL_DB.OPS.PIPELINE_RUN_METRICS

Metrics include:
- files_discovered
- files_to_load
- data_files_loaded
- manifests_loaded
- dbt run duration (seconds)
- dbt test duration (seconds)
- dbt test pass/fail state

The incremental loader prints a final JSON line to stdout, which Airflow captures via XCom.


REPOSITORY STRUCTURE


- infra/
  - docker-compose.yml (MinIO + Airflow + Postgres)
  - airflow/Dockerfile (custom Airflow image with deps pinned)

- airflow/
  - dags/ (Airflow DAGs)
  - logs/
  - plugins/

- ingestion/
  - src/ (API ingestion, MinIO utilities, Snowflake loaders, load-state logic)

- dbt/
  - profiles.yml (repo-local dbt profiles used in Airflow containers)
  - football_dbt/ (dbt project: models, macros, tests)


LOCAL RUNBOOK (MANUAL)


A) Start infra services (MinIO + Airflow + Postgres)
- Run from infra/ directory:
  docker compose up -d

B) Daily manual update (outside Airflow)
1) python -m ingestion.src.ingest_matches_incremental
2) python -m ingestion.src.load_incremental_matches_to_snowflake
3) dbt run
4) dbt test

C) Airflow (preferred)
- Trigger football_daily_pipeline or let schedule run.


SNOWFLAKE ACCESS NOTES (DASHBOARDS / QUERIES)


If you see “Insufficient privileges to operate on table …” in Snowsight, you need:
- SELECT on SILVER and GOLD tables/views for your current role, OR
- switch role to one that owns/has access (e.g., FOOTBALL_PIPELINE_ROLE), OR
- grant USAGE on DB/SCHEMA + SELECT on ALL/FUTURE TABLES/VIEWs in SILVER/GOLD to your role.

A recommended next improvement is to create a read-only analyst role (e.g., FOOTBALL_ANALYST_ROLE)
and use that for Snowsight dashboards/BI tools.


NEXT PLANNED ITEMS


- Build Snowflake-native dashboards in Snowsight (charts + dashboard tiles)
- Add Metabase later (hosted) for external BI sharing
- Improve observability for backfill DAG (same metrics pattern)
- Add alerting on DAG failures (email/Slack)
- Create FOOTBALL_ANALYST_ROLE for clean read-only access
