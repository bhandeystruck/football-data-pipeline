from .snowflake_client import get_snowflake_conn
from .logger import get_logger

logger = get_logger("test_snowflake_connection")

def main():
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
        row = cur.fetchone()
        logger.info(f"Connected OK: user={row[0]}, role={row[1]}, wh={row[2]}, db={row[3]}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()