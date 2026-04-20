import requests
import json
import argparse
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import os
import logging

# Setup logging so we can see what's happening
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _parse_date_arg(date_str):
    """Parse YYYYMMDD date string, return datetime. Raises ValueError on bad format."""
    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        raise ValueError(f"Invalid date format '{date_str}'. Expected YYYYMMDD (e.g. 20260101).")


def fetch_news_from_alpha_vantage(ticker, api_key, start_date=None, end_date=None, limit=50):
    """
    Fetch news and sentiment from Alpha Vantage API.

    Args:
        ticker: Stock symbol (e.g., 'AAPL')
        api_key: Your Alpha Vantage API key
        start_date: datetime or None — if provided, sets time_from (inclusive)
        end_date: datetime or None — if provided, sets time_to (inclusive, end of day)
        limit: max articles to return (default 50; use 1000 for backfills on premium tier)

    Returns:
        dict: Raw API response containing news articles

    Raises:
        requests.HTTPError: On HTTP 4xx/5xx responses
        Exception: If API returns an application-level error or rate limit notice
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": ticker,
        "apikey": api_key,
        "limit": limit,
    }

    if start_date is not None:
        params["time_from"] = start_date.strftime("%Y%m%dT%H%M")
    if end_date is not None:
        # Use end of day so the full end_date is included
        params["time_to"] = end_date.strftime("%Y%m%dT2359")

    date_range = ""
    if start_date or end_date:
        date_range = f" ({params.get('time_from', '*')} → {params.get('time_to', '*')})"
    logger.info(f"Fetching news for {ticker}{date_range}...")

    response = requests.get(url, params=params, timeout=30)

    if response.status_code == 429:
        raise Exception("RATE_LIMIT: Alpha Vantage daily API limit reached (HTTP 429).")

    response.raise_for_status()

    body = response.json()

    if "error message" in body:
        raise Exception(f"Alpha Vantage API error: {body['error message']}")

    if "Note" in body:
        raise Exception(f"RATE_LIMIT: Alpha Vantage rate limit hit: {body['Note']}")

    if "Information" in body:
        raise Exception(f"RATE_LIMIT: Alpha Vantage rate limit hit: {body['Information']}")

    logger.info(f"Successfully fetched news for {ticker}")
    return body


def save_to_minio(data, ticker, minio_endpoint, access_key, secret_key, start_date=None):
    """
    Save JSON data to MinIO (S3-compatible storage).

    Args:
        data: Python dict to save as JSON
        ticker: Stock symbol (used in filename)
        minio_endpoint: MinIO endpoint (e.g., 'http://minio:9000')
        access_key: MinIO access key
        secret_key: MinIO secret key
        start_date: datetime or None — included in filename for backfill files

    Returns:
        str: The filename that was saved
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    try:
        s3.head_bucket(Bucket="raw-news")
    except ClientError:
        logger.info("Creating raw-news bucket...")
        s3.create_bucket(Bucket="raw-news")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if start_date is not None:
        filename = f"{ticker}_{start_date.strftime('%Y%m%d')}_{timestamp}.json"
    else:
        filename = f"{ticker}_{timestamp}.json"

    s3.put_object(
        Bucket="raw-news",
        Key=filename,
        Body=json.dumps(data, indent=2),
    )

    logger.info(f"Saved to MinIO: s3://raw-news/{filename}")
    return filename


def fetch_and_store_news(
    ticker,
    api_key,
    minio_endpoint,
    minio_access_key,
    minio_secret_key,
    start_date=None,
    end_date=None,
    limit=50,
):
    """
    Complete pipeline: fetch news from API, save to MinIO.

    Args:
        ticker: Stock symbol
        api_key: Alpha Vantage API key
        minio_endpoint / minio_access_key / minio_secret_key: MinIO credentials
        start_date: datetime or None
        end_date: datetime or None

    Returns:
        dict with keys:
            success      — True if both API fetch and MinIO save succeeded
            api_called   — True if Alpha Vantage was contacted (quota was consumed)
            filename     — set on success
            error        — set on failure
            rate_limited — True if the failure was an API rate limit
    """
    api_called = False
    try:
        news_data = fetch_news_from_alpha_vantage(ticker, api_key, start_date, end_date, limit=limit)
        api_called = True  # quota consumed from this point on

        filename = save_to_minio(
            data=news_data,
            ticker=ticker,
            minio_endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            start_date=start_date,
        )

        logger.info(f"Successfully fetched and stored news for {ticker}")
        return {"success": True, "api_called": True, "filename": filename}

    except Exception as e:
        error_msg = str(e)
        is_rate_limit = error_msg.startswith("RATE_LIMIT:")
        logger.error(f"Error fetching/storing news for {ticker}: {error_msg}")
        return {
            "success": False,
            "api_called": api_called,
            "error": error_msg,
            "rate_limited": is_rate_limit,
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch news sentiment from Alpha Vantage and store in MinIO."
    )
    parser.add_argument("--ticker", default="AAPL", help="Stock ticker symbol (default: AAPL)")
    parser.add_argument(
        "--start-date",
        metavar="YYYYMMDD",
        help="Start of date range (inclusive). Omit for latest news.",
    )
    parser.add_argument(
        "--end-date",
        metavar="YYYYMMDD",
        help="End of date range (inclusive). Omit for latest news.",
    )
    args = parser.parse_args()

    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    start_date = _parse_date_arg(args.start_date) if args.start_date else None
    end_date = _parse_date_arg(args.end_date) if args.end_date else None

    result = fetch_and_store_news(
        args.ticker,
        api_key,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        start_date=start_date,
        end_date=end_date,
    )

    if result["success"]:
        print(f"SUCCESS: {result['filename']}")
    else:
        print(f"FAILED: {result['error']}")
        exit(1)
