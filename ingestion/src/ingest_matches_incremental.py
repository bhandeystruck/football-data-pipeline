# ingestion/src/ingest_matches_incremental.py

import os
import uuid
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

from .api_client import get_json
from .bronze_writer import put_bronze_json
from .config import MINIO_BRONZE_BUCKET
from .logger import get_logger

logger = get_logger("ingest_matches_incremental")


def _load_targets() -> list[str]:
    """
    Loads comma-separated competition codes from repo-root .env.
    Example: TARGET_COMPETITION_CODES=PL,SA,BL1
    """
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
    raw = (os.getenv("TARGET_COMPETITION_CODES") or "").strip()
    if not raw:
        raise RuntimeError("Missing TARGET_COMPETITION_CODES in .env (e.g., TARGET_COMPETITION_CODES=PL)")
    return [x.strip() for x in raw.split(",") if x.strip()]


def main():
    run_id = str(uuid.uuid4())
    now_utc = datetime.now(timezone.utc)

    # Incremental window: yesterday -> next 7 days
    date_from = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")
    date_to = (now_utc + timedelta(days=7)).strftime("%Y-%m-%d")
    dt_partition = now_utc.strftime("%Y-%m-%d")

    targets = _load_targets()

    logger.info(f"Targets: {targets}")
    logger.info(f"Window: dateFrom={date_from}, dateTo={date_to}")

    for code in targets:
        logger.info(f"Fetching matches for competition={code} ...")

        payload = get_json(
            path=f"/competitions/{code}/matches",
            params={"dateFrom": date_from, "dateTo": date_to},
        )

        match_count = len(payload.get("matches", []))

        # Bronze path (partitioned)
        data_key = (
            f"endpoint=matches/"
            f"competition={code}/"
            f"dateFrom={date_from}/"
            f"dateTo={date_to}/"
            f"dt={dt_partition}/"
            f"run_id={run_id}.json"
        )

        # 1) Write raw payload
        put_bronze_json(data_key, payload)
        logger.info(f"Saved {match_count} matches to s3://{MINIO_BRONZE_BUCKET}/{data_key}")

        # 2) Write manifest next to it
        manifest_key = data_key.replace(".json", ".manifest.json")
        manifest = {
            "run_id": run_id,
            "endpoint": "matches",
            "competition": code,
            "params": {"dateFrom": date_from, "dateTo": date_to},
            "dt_partition": dt_partition,
            "fetched_at_utc": now_utc.isoformat(),
            "record_count": match_count,
            "bucket": MINIO_BRONZE_BUCKET,
            "data_key": data_key,
        }

        put_bronze_json(manifest_key, manifest)
        logger.info(f"Wrote manifest to s3://{MINIO_BRONZE_BUCKET}/{manifest_key}")

    logger.info("✅ Incremental matches ingestion complete.")


if __name__ == "__main__":
    main()