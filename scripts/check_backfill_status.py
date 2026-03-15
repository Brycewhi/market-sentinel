"""
check_backfill_status.py — Show current backfill progress at a glance.

Usage:
    python check_backfill_status.py
"""

import json
from datetime import datetime
from pathlib import Path

PROGRESS_FILE = Path(__file__).parent / "backfill_progress.json"
TOTAL_CALLS = 78   # 3 tickers × 26 weeks
DAILY_LIMIT = 25


def main():
    if not PROGRESS_FILE.exists():
        print("No backfill started yet.")
        print("Run: python backfill_news.py")
        return

    with open(PROGRESS_FILE) as f:
        progress = json.load(f)

    completed = progress.get("completed_calls", 0)
    last_date = progress.get("last_completed_date", "—")
    failed = progress.get("failed_calls", [])
    history = progress.get("history", [])

    remaining = TOTAL_CALLS - completed
    pct = 100 * completed // TOTAL_CALLS if TOTAL_CALLS else 0
    days_left = -(-remaining // DAILY_LIMIT)  # ceiling division

    print("─" * 50)
    print("  Market Sentinel — Backfill Status")
    print("─" * 50)
    print(f"  Completed:  {completed}/{TOTAL_CALLS} calls ({pct}%)")
    print(f"  Last run:   {last_date}")
    if failed:
        print(f"  Failed:     {len(failed)} call(s)")
    if remaining > 0:
        print(f"  Remaining:  {remaining} calls")
        print(f"  Est. days:  {days_left}")
    else:
        print("  Status:     COMPLETE")
    print("─" * 50)

    # Per-ticker breakdown
    if history:
        tickers = sorted({e["ticker"] for e in history})
        print("\nPer-ticker:")
        for ticker in tickers:
            done = sum(1 for e in history if e["ticker"] == ticker and e["status"] == "SUCCESS")
            fail = sum(1 for e in history if e["ticker"] == ticker and e["status"] == "FAILED")
            total_weeks = 26
            line = f"  {ticker}: {done}/{total_weeks} weeks"
            if fail:
                line += f"  ({fail} failed)"
            print(line)

    # Recent history (last 9 entries)
    if history:
        print("\nRecent activity:")
        for entry in history[-9:]:
            status_sym = "✓" if entry["status"] == "SUCCESS" else "✗"
            print(f"  {status_sym} {entry['date']}  {entry['ticker']}  {entry['status']}")

    if failed:
        print(f"\nFailed calls (check backfill_progress.json for details):")
        for f_entry in failed[-5:]:
            print(f"  ✗ {f_entry['date']}  {f_entry['ticker']}")
        if len(failed) > 5:
            print(f"  ... and {len(failed) - 5} more")


if __name__ == "__main__":
    main()
