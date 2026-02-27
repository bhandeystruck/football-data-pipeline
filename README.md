# football-data-pipeline

Production-style data engineering project using football-data.org (REST API) → MinIO (Bronze) → Snowflake (Warehouse) → dbt (Silver/Gold) → Airflow (Orchestration).

## Current Progress
- MinIO running via Docker Compose
- Bronze ingestion working for `/competitions`
- Added retries + rate-limit handling and structured logging

## Local Setup
1. Start MinIO:
   ```bash
   cd infra
   docker compose up -d