import os
import uuid
from datetime import datetime, timezone, timedelta, date

from dotenv import load_dotenv

from .api_client import get_json
from .bronze_writer import put_bronze_json
from .config import MINIO_BRONZE_BUCKET
from .logger import get_logger

logger = get_logger("ingest_matches_backfill")


def _load_env():
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

    comp = os.getenv("BACKFILL_COMPETITION_CODE", "PL").strip()
    start = os.getenv("BACKFILL_START_DATE")
    end = os.getenv("BACKFILL_END_DATE")
    chunk_days = int(os.getenv("BACKFILL_CHUNK_DAYS", "30"))

    if not start or not end:
        raise RuntimeError("BACKFILL_START_DATE and BACKFILL_END_DATE must be set in .env")

    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    if start_d > end_d:
        raise RuntimeError("BACKFILL_START_DATE must be <= BACKFILL_END_DATE")

    if chunk_days < 1 or chunk_days > 60:
        raise RuntimeError("BACKFILL_CHUNK_DAYS should be between 1 and 60")

    return comp, start_d, end_d, chunk_days


def main():
    competition, start_d, end_d, chunk_days = _load_env()

    run_id = str(uuid.uuid4())
    now_utc = datetime.now(timezone.utc)
    dt_partition = now_utc.strftime("%Y-%m-%d")

    logger.info(f"Backfill competition={competition}, range={start_d}..{end_d}, chunk_days={chunk_days}")

    cur = start_d
    chunk_num = 0

    while cur <= end_d:
        chunk_num += 1
        chunk_start = cur
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end_d)

        date_from = chunk_start.isoformat()
        date_to = chunk_end.isoformat()

        logger.info(f"[{chunk_num}] Fetching chunk dateFrom={date_from} dateTo={date_to}")

        payload = get_json(
            path=f"/competitions/{competition}/matches",
            params={"dateFrom": date_from, "dateTo": date_to},
        )

        match_count = len(payload.get("matches", []))

        data_key = (
            f"endpoint=matches_backfill/"
            f"competition={competition}/"
            f"dateFrom={date_from}/"
            f"dateTo={date_to}/"
            f"dt={dt_partition}/"
            f"run_id={run_id}.json"
        )

        put_bronze_json(data_key, payload)

        manifest_key = data_key.replace(".json", ".manifest.json")
        manifest = {
            "run_id": run_id,
            "endpoint": "matches_backfill",
            "competition": competition,
            "params": {"dateFrom": date_from, "dateTo": date_to},
            "dt_partition": dt_partition,
            "fetched_at_utc": now_utc.isoformat(),
            "record_count": match_count,
            "bucket": MINIO_BRONZE_BUCKET,
            "data_key": data_key,
        }
        put_bronze_json(manifest_key, manifest)

        logger.info(f"[{chunk_num}] Saved chunk: matches={match_count}")
        cur = chunk_end + timedelta(days=1)

    logger.info("✅ Backfill ingestion complete.")


if __name__ == "__main__":
    main()