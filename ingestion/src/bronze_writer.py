import json
from typing import Any

from .minio_client import get_s3_client
from .config import MINIO_BRONZE_BUCKET

def put_json(bucket: str, key: str, payload: Any) -> None:
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )

def put_bronze_json(key: str, payload: Any) -> None:
    put_json(MINIO_BRONZE_BUCKET, key, payload)