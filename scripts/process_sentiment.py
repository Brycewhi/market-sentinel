"""
process_sentiment.py - Analyze sentiment using FinBERT and save to PostgreSQL
"""

import os
import json
import boto3
from datetime import datetime
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


# Configuration
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'raw-news'

DB_CONNECTION_STRING = 'postgresql://market_sentinel:market_sentinel_password@postgres:5432/market_sentinel'

# Model configuration
MODEL_NAME = "ProsusAI/finbert"
BATCH_SIZE = 32

# Load FinBERT model once
print("🤖 Loading FinBERT model...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
model.eval()
print("✅ FinBERT model loaded and cached")


def parse_alpha_vantage_timestamp(time_str):
    """
    Convert Alpha Vantage timestamp to Python datetime.
    Format: '20260213T182853' → datetime(2026, 2, 13, 18, 28, 53)
    """
    if not time_str:
        return None
    try:
        # Parse format: YYYYMMDDTHHmmss
        return datetime.strptime(time_str, '%Y%m%dT%H%M%S')
    except:
        return None


def get_latest_news_file(ticker):
    """Get the most recent news file for a ticker from MinIO."""
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=ticker)
    
    if 'Contents' not in response:
        print(f"⚠️  No files found for {ticker}")
        return None, None
    
    files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
    
    if not files:
        return None, None
    
    latest_file = files[0]['Key']
    print(f"📄 Found latest file: {latest_file}")
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=latest_file)
    content = obj['Body'].read().decode('utf-8')
    
    return latest_file, content


def analyze_sentiment_batch(headlines):
    """Analyze sentiment for a batch of headlines using FinBERT."""
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


def save_to_database(ticker, articles_with_sentiment):
    """Save sentiment analysis results to PostgreSQL with timestamps."""
    engine = create_engine(DB_CONNECTION_STRING)
    
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
                    'published_at': item.get('published_at'),  # ← NEW!
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
                print(f"⚠️  Error inserting headline: {str(e)}")
                continue
    
    print(f"✅ Inserted {inserted_count} new records, skipped {duplicate_count} duplicates")
    
    return inserted_count


def main(ticker):
    """Main function: Process sentiment for latest news file."""
    try:
        print(f"🔍 Processing sentiment for {ticker}...")
        filename, content = get_latest_news_file(ticker)
        
        if not filename:
            print(f"⚠️  No news files found for {ticker}")
            return
        
        data = json.loads(content)
        articles = data.get('feed', [])
        
        if not articles:
            print(f"⚠️  No articles in file {filename}")
            return
        
        print(f"📊 Found {len(articles)} articles")
        
        # Extract articles with metadata
        articles_data = []
        for article in articles:
            if article.get('title'):
                articles_data.append({
                    'headline': article.get('title', ''),
                    'url': article.get('url', ''),
                    'source': article.get('source', ''),
                    'time_published': article.get('time_published', '')  # ← Extract timestamp
                })
        
        print(f"📰 Extracted {len(articles_data)} articles with metadata")
        
        # Analyze sentiment in batches
        print(f"🤖 Analyzing sentiment with FinBERT (batch size: {BATCH_SIZE})...")
        all_results = []
        
        for i in range(0, len(articles_data), BATCH_SIZE):
            batch_articles = articles_data[i:i + BATCH_SIZE]
            batch_headlines = [a['headline'] for a in batch_articles]
            
            batch_results = analyze_sentiment_batch(batch_headlines)
            
            # Combine metadata with sentiment scores
            for article, scores in zip(batch_articles, batch_results):
                all_results.append({
                    'headline': article['headline'],
                    'url': article['url'],
                    'source': article['source'],
                    'published_at': parse_alpha_vantage_timestamp(article['time_published']),  # ← Parse timestamp
                    'positive': scores['positive'],
                    'negative': scores['negative'],
                    'neutral': scores['neutral']
                })
            
            print(f"  ✓ Processed batch {i//BATCH_SIZE + 1}/{(len(articles_data)-1)//BATCH_SIZE + 1}")
        
        print(f"✅ Sentiment analysis complete: {len(all_results)} articles analyzed")
        
        # Save to database
        print(f"💾 Saving to database...")
        inserted = save_to_database(ticker, all_results)
        
        print(f"🎉 Pipeline completed for {ticker}: {inserted} new records")
        
        return inserted
        
    except Exception as e:
        print(f"❌ Error processing {ticker}: {str(e)}")
        raise


if __name__ == "__main__":
    import sys
    ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    main(ticker)
