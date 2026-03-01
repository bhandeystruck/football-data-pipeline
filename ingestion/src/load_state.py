from typing import Set, Optional

from .logger import get_logger
from .snowflake_client import get_snowflake_conn

logger = get_logger("load_state")

LOAD_STATE_FQN = "FOOTBALL_DB.BRONZE.LOAD_STATE"


def get_loaded_keys(prefix: str) -> Set[str]:
    """
    Returns file_keys already loaded that start with a given prefix.
    Example prefix: 'endpoint=matches_backfill/'
    """
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT FILE_KEY
            FROM {LOAD_STATE_FQN}
            WHERE FILE_KEY LIKE %s
            """,
            (prefix + "%",),
        )
        return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def mark_loaded(file_key: str, endpoint: Optional[str] = None) -> None:
    """
    Insert file_key into load state (idempotent).
    """
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            MERGE INTO {LOAD_STATE_FQN} t
            USING (SELECT %s AS FILE_KEY, %s AS ENDPOINT) s
            ON t.FILE_KEY = s.FILE_KEY
            WHEN NOT MATCHED THEN
              INSERT (FILE_KEY, ENDPOINT) VALUES (s.FILE_KEY, s.ENDPOINT)
            """,
            (file_key, endpoint),
        )
        conn.commit()
    finally:
        conn.close()