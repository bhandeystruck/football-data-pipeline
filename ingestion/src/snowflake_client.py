import os
from dotenv import load_dotenv
import snowflake.connector

# Load repo-root .env from inside ingestion/src/
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

def get_snowflake_conn():
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    role = os.getenv("SNOWFLAKE_ROLE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")

    missing = [k for k, v in {
        "SNOWFLAKE_ACCOUNT": account,
        "SNOWFLAKE_USER": user,
        "SNOWFLAKE_PASSWORD": password,
        "SNOWFLAKE_ROLE": role,
        "SNOWFLAKE_WAREHOUSE": warehouse,
        "SNOWFLAKE_DATABASE": database,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required Snowflake env vars: {missing}")

    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database,
    )