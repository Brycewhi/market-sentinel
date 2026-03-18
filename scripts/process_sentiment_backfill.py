"""
process_sentiment_backfill.py - Process ALL historical backfilled files in MinIO
"""

import os
import re
import json
import boto3
from datetime import datetime
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from sqlalchemy import create_engine, text


# Configuration
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'raw-news'

DB_CONNECTION_STRING = 'postgresql://market_sentinel:market_sentinel_password@postgres:5432/market_sentinel'

MODEL_NAME = "ProsusAI/finbert"
BATCH_SIZE = 32

# Pattern: TICKER_YYYYMMDD_timestamp.json (daily) or TICKER_YYYYMMDD_YYYYMMDD_timestamp.json (weekly backfill)
BACKFILL_PATTERN = re.compile(r'^([A-Z]+)_(\d{8})_(?:\d{8}_)?\d+\.json$')

# Load FinBERT model once
print("Loading FinBERT model...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
model.eval()
print("FinBERT model loaded")


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def list_backfill_files(s3_client):
    """List all backfilled files matching TICKER_YYYYMMDD_timestamp.json pattern."""
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if BACKFILL_PATTERN.match(key):
                files.append(key)
    return sorted(files)


def parse_alpha_vantage_timestamp(time_str):
    if not time_str:
        return None
    try:
        return datetime.strptime(time_str, '%Y%m%dT%H%M%S')
    except Exception:
        return None


def analyze_sentiment_batch(headlines):
    if not headlines:
        return []

    inputs = tokenizer(
        headlines,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=512
    )

    with torch.no_grad():
        outputs = model(**inputs)

    probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)

    results = []
    for probs in probabilities:
        results.append({
            'positive': probs[0].item(),
            'negative': probs[1].item(),
            'neutral': probs[2].item()
        })

    return results


def save_to_database(engine, ticker, articles_with_sentiment):
    inserted_count = 0
    duplicate_count = 0

    with engine.begin() as conn:
        for item in articles_with_sentiment:
            try:
                query = text("""
                    INSERT INTO staging.sentiment_logs
                    (ticker, headline, url, source, published_at, positive_score, negative_score, neutral_score, created_at)
                    VALUES (:ticker, :headline, :url, :source, :published_at, :positive, :negative, :neutral, :created_at)
                    ON CONFLICT (ticker, headline) DO NOTHING
                """)

                result = conn.execute(query, {
                    'ticker': ticker,
                    'headline': item['headline'],
                    'url': item.get('url'),
                    'source': item.get('source'),
                    'published_at': item.get('published_at'),
                    'positive': item['positive'],
                    'negative': item['negative'],
                    'neutral': item['neutral'],
                    'created_at': datetime.now()
                })

                if result.rowcount > 0:
                    inserted_count += 1
                else:
                    duplicate_count += 1

            except Exception as e:
                print(f"  Warning: error inserting headline: {e}")
                continue

    return inserted_count, duplicate_count


def process_file(s3_client, engine, key, index, total):
    match = BACKFILL_PATTERN.match(key)
    ticker = match.group(1)

    print(f"[{index}/{total}] Processing {key}...", end=" ", flush=True)

    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    content = obj['Body'].read().decode('utf-8')
    data = json.loads(content)
    articles = data.get('feed', [])

    if not articles:
        print("(no articles)")
        return 0

    articles_data = [
        {
            'headline': a.get('title', ''),
            'url': a.get('url', ''),
            'source': a.get('source', ''),
            'time_published': a.get('time_published', '')
        }
        for a in articles if a.get('title')
    ]

    all_results = []
    for i in range(0, len(articles_data), BATCH_SIZE):
        batch = articles_data[i:i + BATCH_SIZE]
        scores = analyze_sentiment_batch([a['headline'] for a in batch])
        for article, s in zip(batch, scores):
            all_results.append({
                'headline': article['headline'],
                'url': article['url'],
                'source': article['source'],
                'published_at': parse_alpha_vantage_timestamp(article['time_published']),
                'positive': s['positive'],
                'negative': s['negative'],
                'neutral': s['neutral']
            })

    inserted, dupes = save_to_database(engine, ticker, all_results)
    print(f"✓ {len(all_results)} articles ({inserted} new, {dupes} dupes)")
    return inserted


def main():
    s3_client = get_s3_client()
    engine = create_engine(DB_CONNECTION_STRING)

    print("Scanning MinIO for backfilled files...")
    files = list_backfill_files(s3_client)
    total = len(files)

    if not files:
        print("No backfilled files found.")
        return

    print(f"Found {total} backfilled files\n")

    total_inserted = 0
    for i, key in enumerate(files, start=1):
        try:
            inserted = process_file(s3_client, engine, key, i, total)
            total_inserted += inserted
        except Exception as e:
            print(f"  ERROR: {e}")

    print(f"\nDone. Total new articles inserted: {total_inserted}")


if __name__ == "__main__":
    main()
