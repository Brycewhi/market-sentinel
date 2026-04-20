"""
backfill_news.py — Fetch 6 months of news data in 3 API calls (one per ticker).

Usage:
    python backfill_news.py
"""

import os
import sys
import time
from datetime import datetime

from fetch_news import fetch_and_store_news

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TICKERS = ["AAPL", "MSFT", "GOOGL"]
BACKFILL_START = datetime(2025, 12, 3)
BACKFILL_END = datetime(2025, 12, 31)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_backfill():
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        print("ERROR: ALPHA_VANTAGE_API_KEY environment variable not set.")
        sys.exit(1)

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    print(f"Backfill: {BACKFILL_START.date()} → {BACKFILL_END.date()}")
    print(f"Tickers: {', '.join(TICKERS)} ({len(TICKERS)} API calls total)\n")

    succeeded = 0
    failed = 0

    for i, ticker in enumerate(TICKERS, 1):
        if i > 1:
            time.sleep(3)  # Alpha Vantage: 1 req/sec burst limit

        print(f"[{i}/{len(TICKERS)}] Fetching {ticker}... ", end="", flush=True)

        result = fetch_and_store_news(
            ticker=ticker,
            api_key=api_key,
            minio_endpoint=minio_endpoint,
            minio_access_key=minio_access_key,
            minio_secret_key=minio_secret_key,
            start_date=BACKFILL_START,
            end_date=BACKFILL_END,
            limit=1000,
        )

        if result["success"]:
            print("SUCCESS")
            succeeded += 1
        else:
            print(f"FAILED: {result['error']}")
            failed += 1

    print(f"\nDone. {succeeded}/{len(TICKERS)} succeeded, {failed} failed.")


if __name__ == "__main__":
    run_backfill()
