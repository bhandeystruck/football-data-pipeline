import os
from dotenv import load_dotenv

# Load repo-root .env from inside ingestion/src/
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

FOOTBALL_DATA_API_TOKEN = os.getenv("FOOTBALL_DATA_API_TOKEN")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "football-bronze")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

missing = []
for k, v in {
    "FOOTBALL_DATA_API_TOKEN": FOOTBALL_DATA_API_TOKEN,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
}.items():
    if not v:
        missing.append(k)

if missing:
    raise RuntimeError(f"Missing required env vars: {missing}")