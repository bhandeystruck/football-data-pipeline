import json
from typing import Any, Optional

from .config import MINIO_BRONZE_BUCKET
from .minio_client import get_s3_client

def list_objects(prefix: str) -> list[str]:
    """
    List object keys under a prefix in the bronze bucket.
    """
    s3 = get_s3_client()
    keys: list[str] = []
    token: Optional[str] = None

    while True:
        kwargs = {"Bucket": MINIO_BRONZE_BUCKET, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []):
            keys.append(item["Key"])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return keys

def get_json_object(key: str) -> Any:
    """
    Download an object from MinIO and parse JSON.
    """
    s3 = get_s3_client()
    resp = s3.get_object(Bucket=MINIO_BRONZE_BUCKET, Key=key)
    raw = resp["Body"].read().decode("utf-8")
    return json.loads(raw)