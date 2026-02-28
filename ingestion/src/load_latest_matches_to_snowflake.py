import re

from .logger import get_logger
from .minio_reader import list_objects, get_json_object
from .bronze_snowflake_loader import upsert_raw_matches, upsert_raw_manifest

logger = get_logger("load_latest_matches_to_snowflake")

RUN_ID_RE = re.compile(r"run_id=([a-f0-9\-]+)\.json$")
DT_RE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})")
COMP_RE = re.compile(r"competition=([^/]+)/")
DATEFROM_RE = re.compile(r"dateFrom=(\d{4}-\d{2}-\d{2})")
DATETO_RE = re.compile(r"dateTo=(\d{4}-\d{2}-\d{2})")

def main():
    prefix = "endpoint=matches/"
    data_keys = [k for k in list_objects(prefix) if k.endswith(".json") and not k.endswith(".manifest.json")]
    if not data_keys:
        raise RuntimeError(f"No matches JSON files found in MinIO with prefix: {prefix}")

    latest_key = sorted(data_keys)[-1]
    logger.info(f"Latest matches key: {latest_key}")

    payload = get_json_object(latest_key)

    run_id = RUN_ID_RE.search(latest_key).group(1) if RUN_ID_RE.search(latest_key) else None
    dt = DT_RE.search(latest_key).group(1) if DT_RE.search(latest_key) else None
    competition = COMP_RE.search(latest_key).group(1) if COMP_RE.search(latest_key) else None
    date_from = DATEFROM_RE.search(latest_key).group(1) if DATEFROM_RE.search(latest_key) else None
    date_to = DATETO_RE.search(latest_key).group(1) if DATETO_RE.search(latest_key) else None

    upsert_raw_matches(
        file_key=latest_key,
        competition_code=competition,
        date_from=date_from,
        date_to=date_to,
        run_id=run_id,
        dt=dt,
        payload=payload,
    )

    # load matching manifest if present
    manifest_key = latest_key.replace(".json", ".manifest.json")
    all_keys = set(list_objects(prefix))
    if manifest_key in all_keys:
        manifest = get_json_object(manifest_key)
        upsert_raw_manifest(
            file_key=manifest_key,
            endpoint=manifest.get("endpoint"),
            run_id=manifest.get("run_id"),
            dt=manifest.get("dt_partition"),
            manifest=manifest,
        )
        logger.info(f"✅ Loaded manifest too: {manifest_key}")
    else:
        logger.warning(f"Manifest not found for latest matches file. Expected: {manifest_key}")

    logger.info("✅ Loaded latest matches into Snowflake BRONZE.RAW_MATCHES")

if __name__ == "__main__":
    main()