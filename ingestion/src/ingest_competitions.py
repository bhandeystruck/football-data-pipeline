import json
import uuid
from datetime import datetime, timezone

from .api_client import get_json
from .config import MINIO_BRONZE_BUCKET
from .minio_client import get_s3_client
from .logger import get_logger

logger = get_logger("ingest_competitions")

def main():
    run_id = str(uuid.uuid4())
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # 1) Call API with retries/rate-limit handling
    payload = get_json("/competitions")

    # 2) Write raw JSON to Bronze (MinIO)
    key = f"endpoint=competitions/dt={dt}/run_id={run_id}.json"

    s3 = get_s3_client()
    s3.put_object(
        Bucket=MINIO_BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(f"Saved competitions payload to s3://{MINIO_BRONZE_BUCKET}/{key}")
    logger.info(f"competitions count: {len(payload.get('competitions', []))}")

if __name__ == "__main__":
    main()