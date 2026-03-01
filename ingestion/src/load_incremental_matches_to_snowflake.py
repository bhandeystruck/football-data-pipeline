import re

from .logger import get_logger
from .minio_reader import list_objects, get_json_object
from .load_state import get_loaded_keys, mark_loaded
from .bronze_snowflake_loader import upsert_raw_matches, upsert_raw_manifest

logger = get_logger("load_incremental_matches_to_snowflake")

RUN_ID_RE = re.compile(r"run_id=([a-f0-9\-]+)\.(?:manifest\.)?json$")
DT_RE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})")
COMP_RE = re.compile(r"competition=([^/]+)/")
DATEFROM_RE = re.compile(r"dateFrom=(\d{4}-\d{2}-\d{2})")
DATETO_RE = re.compile(r"dateTo=(\d{4}-\d{2}-\d{2})")


def _parse_key(key: str):
    run_id = RUN_ID_RE.search(key).group(1) if RUN_ID_RE.search(key) else None
    dt = DT_RE.search(key).group(1) if DT_RE.search(key) else None
    comp = COMP_RE.search(key).group(1) if COMP_RE.search(key) else None
    date_from = DATEFROM_RE.search(key).group(1) if DATEFROM_RE.search(key) else None
    date_to = DATETO_RE.search(key).group(1) if DATETO_RE.search(key) else None
    return run_id, dt, comp, date_from, date_to


def main():
    prefix = "endpoint=matches/"
    keys = list_objects(prefix)

    data_keys = sorted([k for k in keys if k.endswith(".json") and not k.endswith(".manifest.json")])
    manifest_keys = set([k for k in keys if k.endswith(".manifest.json")])

    if not data_keys:
        logger.info(f"No incremental match files found in MinIO with prefix: {prefix}")
        return

    # ✅ Load-state: skip already loaded data files (and manifests indirectly)
    already_loaded = get_loaded_keys(prefix)
    data_keys_to_load = [k for k in data_keys if k not in already_loaded]

    logger.info(f"Found {len(data_keys)} incremental data files")
    logger.info(f"Already loaded (by prefix): {len(already_loaded)}")
    logger.info(f"To load now: {len(data_keys_to_load)}")

    loaded_data = 0
    loaded_manifests = 0

    for i, data_key in enumerate(data_keys_to_load, start=1):
        run_id, dt, comp, date_from, date_to = _parse_key(data_key)

        logger.info(f"[{i}/{len(data_keys_to_load)}] Loading data_key={data_key}")
        payload = get_json_object(data_key)

        # 1) Upsert data payload into RAW_MATCHES
        upsert_raw_matches(
            file_key=data_key,
            competition_code=comp,
            date_from=date_from,
            date_to=date_to,
            run_id=run_id,
            dt=dt,
            payload=payload,
        )
        loaded_data += 1

        # 2) Mark the data file as loaded
        mark_loaded(data_key, endpoint="matches_incremental")

        # 3) Load matching manifest (if present)
        expected_manifest_key = data_key.replace(".json", ".manifest.json")
        if expected_manifest_key in manifest_keys:
            manifest = get_json_object(expected_manifest_key)
            manifest_dt = manifest.get("dt_partition") or dt

            upsert_raw_manifest(
                file_key=expected_manifest_key,
                endpoint=manifest.get("endpoint") or "matches",
                run_id=manifest.get("run_id") or run_id,
                dt=manifest_dt,
                manifest=manifest,
            )
            loaded_manifests += 1

            # Mark manifest as loaded too
            mark_loaded(expected_manifest_key, endpoint="matches_incremental_manifest")
        else:
            logger.warning(f"Manifest missing for {data_key} (expected {expected_manifest_key})")

    logger.info(f"✅ Loaded incremental: data_files={loaded_data}, manifests={loaded_manifests}")


if __name__ == "__main__":
    main()