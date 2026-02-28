import json
from typing import Any, Optional

from .logger import get_logger
from .snowflake_client import get_snowflake_conn

logger = get_logger("bronze_snowflake_loader")

RAW_COMPETITIONS_FQN = "FOOTBALL_DB.BRONZE.RAW_COMPETITIONS"
RAW_MATCHES_FQN = "FOOTBALL_DB.BRONZE.RAW_MATCHES"
RAW_MANIFESTS_FQN = "FOOTBALL_DB.BRONZE.RAW_MANIFESTS"


def upsert_raw_competitions(file_key: str, run_id: Optional[str], dt: Optional[str], payload: Any) -> None:
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {RAW_COMPETITIONS_FQN} WHERE FILE_KEY = %s", (file_key,))

        payload_json = json.dumps(payload)
        cur.execute(
            f"""
            INSERT INTO {RAW_COMPETITIONS_FQN} (FILE_KEY, RUN_ID, DT, PAYLOAD)
            SELECT %s, %s, %s::DATE, PARSE_JSON(%s)
            """,
            (file_key, run_id, dt, payload_json),
        )

        conn.commit()
        logger.info(f"Upserted RAW_COMPETITIONS for file_key={file_key}")
    finally:
        conn.close()


def upsert_raw_matches(
    file_key: str,
    competition_code: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    run_id: Optional[str],
    dt: Optional[str],
    payload: Any,
) -> None:
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {RAW_MATCHES_FQN} WHERE FILE_KEY = %s", (file_key,))

        payload_json = json.dumps(payload)
        cur.execute(
            f"""
            INSERT INTO {RAW_MATCHES_FQN}
              (FILE_KEY, COMPETITION_CODE, DATE_FROM, DATE_TO, RUN_ID, DT, PAYLOAD)
            SELECT
              %s, %s, %s::DATE, %s::DATE, %s, %s::DATE, PARSE_JSON(%s)
            """,
            (file_key, competition_code, date_from, date_to, run_id, dt, payload_json),
        )

        conn.commit()
        logger.info(f"Upserted RAW_MATCHES for file_key={file_key}")
    finally:
        conn.close()


def upsert_raw_manifest(
    file_key: str,
    endpoint: Optional[str],
    run_id: Optional[str],
    dt: Optional[str],
    manifest: Any,
) -> None:
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {RAW_MANIFESTS_FQN} WHERE FILE_KEY = %s", (file_key,))

        manifest_json = json.dumps(manifest)
        cur.execute(
            f"""
            INSERT INTO {RAW_MANIFESTS_FQN}
              (FILE_KEY, ENDPOINT, RUN_ID, DT, MANIFEST)
            SELECT
              %s, %s, %s, %s::DATE, PARSE_JSON(%s)
            """,
            (file_key, endpoint, run_id, dt, manifest_json),
        )

        conn.commit()
        logger.info(f"Upserted RAW_MANIFESTS for file_key={file_key}")
    finally:
        conn.close()