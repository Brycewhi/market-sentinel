"""
backfill_news.py — Manual daily backfill for 6 months of news data.

Usage:
    python backfill_news.py

Run once per day. Reads/writes backfill_progress.json to resume where it left off.
Stops after 25 successful API calls (Alpha Vantage free-tier daily limit).
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from fetch_news import fetch_and_store_news

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TICKERS = ["AAPL", "MSFT", "GOOGL"]
BACKFILL_START = datetime(2026, 1, 1)
BACKFILL_END = datetime(2026, 6, 30)
WEEK_DAYS = 7
DAILY_LIMIT = 25
PROGRESS_FILE = Path(__file__).parent / "backfill_progress.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _all_weeks():
    """Yield (week_number, week_start, week_end) for the full backfill range."""
    week_start = BACKFILL_START
    week_num = 1
    while week_start <= BACKFILL_END:
        week_end = min(week_start + timedelta(days=WEEK_DAYS - 1), BACKFILL_END)
        yield week_num, week_start, week_end
        week_start += timedelta(days=WEEK_DAYS)
        week_num += 1


TOTAL_WEEKS = sum(1 for _ in _all_weeks())
TOTAL_CALLS = TOTAL_WEEKS * len(TICKERS)


def _load_progress():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {
        "last_completed_date": None,
        "completed_calls": 0,
        "failed_calls": [],
        "history": [],
    }


def _save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def _already_done(progress, week_start, ticker):
    """Return True if this (week, ticker) combo is already in history as SUCCESS."""
    date_str = week_start.strftime("%Y-%m-%d")
    return any(
        e["date"] == date_str and e["ticker"] == ticker and e["status"] == "SUCCESS"
        for e in progress["history"]
    )


def _resume_from(progress):
    """
    Return the datetime to resume from.
    If last_completed_date is set, resume from that date (history check skips already-done).
    Otherwise start from BACKFILL_START.
    """
    if progress["last_completed_date"]:
        return datetime.strptime(progress["last_completed_date"], "%Y-%m-%d")
    return BACKFILL_START


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_backfill(max_calls=None):
    """
    Args:
        max_calls: Override DAILY_LIMIT for testing (e.g. 3 to test just 1 week).
    """
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        print("ERROR: ALPHA_VANTAGE_API_KEY environment variable not set.")
        sys.exit(1)

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    daily_limit = max_calls if max_calls is not None else DAILY_LIMIT

    progress = _load_progress()
    resume_from = _resume_from(progress)
    calls_today = 0

    if progress["last_completed_date"]:
        print(
            f"Resuming from {progress['last_completed_date']}  "
            f"({progress['completed_calls']}/{TOTAL_CALLS} calls completed overall)"
        )
    else:
        print(f"Starting fresh backfill: {BACKFILL_START.date()} → {BACKFILL_END.date()}")

    if max_calls is not None:
        print(f"TEST MODE: limit={max_calls} calls\n")
    else:
        print(f"Daily limit: {daily_limit} calls\n")

    for week_num, week_start, week_end in _all_weeks():
        # Skip weeks that are entirely before our resume point
        if week_start < resume_from and not _already_done(progress, week_start, TICKERS[0]):
            # If we haven't done any ticker for this week yet and it's before resume, skip
            all_done = all(_already_done(progress, week_start, t) for t in TICKERS)
            if all_done:
                continue
            if week_end < resume_from:
                continue

        print(f"Week {week_num}/{TOTAL_WEEKS}: {week_start.date()} to {week_end.date()}")

        for ticker in TICKERS:
            # Skip if already successfully completed
            if _already_done(progress, week_start, ticker):
                print(f"  {ticker}: already done, skipping")
                continue

            # Check daily limit before making a call
            if calls_today >= daily_limit:
                _save_progress(progress)
                print(f"\nDaily limit reached ({daily_limit} calls).")
                _print_summary(progress, calls_today)
                sys.exit(0)

            print(f"  Fetching {ticker}... ", end="", flush=True)

            result = fetch_and_store_news(
                ticker=ticker,
                api_key=api_key,
                minio_endpoint=minio_endpoint,
                minio_access_key=minio_access_key,
                minio_secret_key=minio_secret_key,
                start_date=week_start,
                end_date=week_end,
            )

            date_str = week_start.strftime("%Y-%m-%d")

            # Count any call that consumed Alpha Vantage quota
            if result.get("api_called"):
                calls_today += 1

            if result["success"]:
                progress["completed_calls"] += 1
                progress["last_completed_date"] = date_str
                progress["history"].append(
                    {"date": date_str, "ticker": ticker, "status": "SUCCESS"}
                )
                print(f"SUCCESS ({calls_today}/{daily_limit} calls today)")

            elif result.get("rate_limited"):
                # Hard stop — save and exit
                progress["failed_calls"].append(
                    {"date": date_str, "ticker": ticker, "error": result["error"]}
                )
                _save_progress(progress)
                print("RATE LIMITED")
                print(f"\nRate limit hit after {calls_today} calls today.")
                _print_summary(progress, calls_today)
                sys.exit(0)

            else:
                # Non-rate-limit failure: log and continue
                progress["failed_calls"].append(
                    {"date": date_str, "ticker": ticker, "error": result["error"]}
                )
                progress["history"].append(
                    {"date": date_str, "ticker": ticker, "status": "FAILED"}
                )
                print(f"FAILED: {result['error']}")

            _save_progress(progress)

            # Small delay between API calls to be polite
            if calls_today < daily_limit:
                time.sleep(1)

    # Reached the end of all weeks
    _save_progress(progress)
    print("\nBackfill complete — all weeks processed.")
    _print_summary(progress, calls_today)


def _print_summary(progress, calls_today):
    completed = progress["completed_calls"]
    remaining = TOTAL_CALLS - completed
    failed = len(progress["failed_calls"])
    days_left = -(-remaining // DAILY_LIMIT)  # ceiling division

    print("─" * 50)
    print(f"Today:     {calls_today} calls made")
    print(f"Overall:   {completed}/{TOTAL_CALLS} calls completed ({100*completed//TOTAL_CALLS}%)")
    if failed:
        print(f"Failed:    {failed} call(s) logged in backfill_progress.json")
    if remaining > 0:
        print(f"Remaining: {remaining} calls (~{days_left} more day(s))")
        print("Run again tomorrow: python backfill_news.py")
    print("─" * 50)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill news data for Market Sentinel.")
    parser.add_argument(
        "--max-calls",
        type=int,
        default=None,
        metavar="N",
        help="Override daily call limit (for testing, e.g. --max-calls 3)",
    )
    args = parser.parse_args()
    run_backfill(max_calls=args.max_calls)
