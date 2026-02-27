import time
import random
import requests

from .config import FOOTBALL_DATA_API_TOKEN
from .logger import get_logger

BASE_URL = "https://api.football-data.org/v4"

logger = get_logger("api_client")

def get_json(path: str, params: dict | None = None, timeout: int = 30, max_retries: int = 5):
    """
    GET a JSON response from football-data.org with:
    - retries on transient errors
    - backoff on 429 and 5xx
    """
    url = f"{BASE_URL}{path}"
    headers = {"X-Auth-Token": FOOTBALL_DATA_API_TOKEN}

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)

            # Handle rate limiting
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else (5 * attempt)
                logger.warning(f"429 Rate limited. Sleeping {sleep_s}s (attempt {attempt}/{max_retries})")
                time.sleep(sleep_s)
                continue

            # Retry server errors
            if 500 <= resp.status_code < 600:
                sleep_s = (2 ** attempt) + random.random()
                logger.warning(f"{resp.status_code} Server error. Sleeping {sleep_s:.1f}s (attempt {attempt}/{max_retries})")
                time.sleep(sleep_s)
                continue

            # Non-retryable client errors
            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.RequestException as e:
            sleep_s = (2 ** attempt) + random.random()
            logger.warning(f"Request failed: {e}. Sleeping {sleep_s:.1f}s (attempt {attempt}/{max_retries})")
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed GET {url} after {max_retries} retries")