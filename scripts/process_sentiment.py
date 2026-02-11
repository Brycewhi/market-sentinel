import json
import logging
import boto3
from datetime import datetime
import sqlalchemy
from sqlalchemy import text
import numpy as np
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinBERTAnalyzer:
    """Load FinBERT once, analyze sentiment for multiple headlines efficiently."""
    
    def __init__(self):
        """Load the FinBERT model (takes ~30 seconds, do this once)."""
        logger.info("Loading FinBERT model (this takes ~30 seconds)...")
        self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        self.model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        logger.info("FinBERT model loaded successfully")
    
    def analyze_batch(self, headlines):
        """
        Analyze sentiment for a batch of headlines.
        
        Args:
            headlines: List of headline strings
        
        Returns:
            List of dicts with sentiment scores: {positive, negative, neutral}
        """
        if not headlines:
            return []
        
        results = []
        
        # Process headlines in batches for efficiency
        batch_size = 32
        for i in range(0, len(headlines), batch_size):
            batch = headlines[i:i + batch_size]
            
            # Tokenize the batch
            inputs = self.tokenizer(
                batch,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=512
            )
            
            # Get predictions
            with torch.no_grad():
                outputs = self.model(**inputs)
                logits = outputs.logits
            
            # Convert logits to probabilities
            probs = torch.nn.functional.softmax(logits, dim=-1)
            
            # Extract scores for this batch
            for j, prob_vector in enumerate(probs):
                sentiment = {
                    'negative': prob_vector[0].item(),
                    'neutral': prob_vector[1].item(),
                    'positive': prob_vector[2].item(),
                }
                results.append(sentiment)
        
        return results

def fetch_raw_news_from_minio(ticker, minio_endpoint, access_key, secret_key):
    """
    Fetch the latest raw news JSON file for a ticker from MinIO.
    
    Returns the parsed JSON data.
    """
    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    
    try:
        # List objects in raw-news bucket, filtered by ticker
        response = s3.list_objects_v2(
            Bucket='raw-news',
            Prefix=f'{ticker}_'
        )
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning(f"No files found for {ticker} in MinIO")
            return None
        
        # Get the most recent file (they're sorted by name, which includes timestamp)
        latest_file = response['Contents'][-1]['Key']
        logger.info(f"Found latest file for {ticker}: {latest_file}")
        
        # Download and parse the file
        obj = s3.get_object(Bucket='raw-news', Key=latest_file)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        
        return data
    
    except Exception as e:
        logger.error(f"Error fetching news from MinIO for {ticker}: {str(e)}")
        raise

def extract_headlines(news_data):
    """
    Extract headlines from Alpha Vantage API response.
    
    Args:
        news_data: Raw API response JSON
    
    Returns:
        List of headline strings
    """
    headlines = []
    
    if 'feed' not in news_data:
        logger.warning("No 'feed' key in news data")
        return headlines
    
    for item in news_data['feed']:
        if 'title' in item:
            headlines.append(item['title'])
    
    logger.info(f"Extracted {len(headlines)} headlines")
    return headlines

def store_sentiment_scores(ticker, headlines, sentiment_scores, db_connection_string):
    """
    Store sentiment scores in PostgreSQL staging table.
    Uses ON CONFLICT to skip duplicates gracefully.
    
    Args:
        ticker: Stock symbol
        headlines: List of headline strings
        sentiment_scores: List of sentiment score dictionaries
        db_connection_string: PostgreSQL connection string
    """
    engine = sqlalchemy.create_engine(db_connection_string)
    
    try:
        inserted_count = 0
        skipped_count = 0
        
        with engine.connect() as conn:
            with conn.begin():  # Start a transaction
                for headline, sentiment in zip(headlines, sentiment_scores):
                    insert_query = text("""
                        INSERT INTO staging.sentiment_logs 
                        (ticker, headline, positive_score, negative_score, neutral_score)
                        VALUES (:ticker, :headline, :positive, :negative, :neutral)
                        ON CONFLICT (ticker, headline) DO NOTHING
                    """)
                    
                    result = conn.execute(insert_query, {
                        'ticker': ticker,
                        'headline': headline,
                        'positive': sentiment['positive'],
                        'negative': sentiment['negative'],
                        'neutral': sentiment['neutral']
                    })
                    
                    if result.rowcount > 0:
                        inserted_count += 1
                    else:
                        skipped_count += 1
            # Transaction auto-commits here
            
            logger.info(f"Stored {inserted_count} new sentiment scores for {ticker} (skipped {skipped_count} duplicates)")
            
    except Exception as e:
        logger.error(f"Error storing sentiment scores: {str(e)}")
        raise

def process_sentiment_for_ticker(ticker, analyzer, minio_endpoint, minio_access_key, 
                                 minio_secret_key, db_connection_string):
    """
    Complete pipeline: fetch raw news → extract headlines → analyze sentiment → store results.
    
    This is the main function that Airflow will call.
    """
    try:
        logger.info(f"Processing sentiment for {ticker}...")
        
        # Step 1: Fetch raw news from MinIO
        news_data = fetch_raw_news_from_minio(ticker, minio_endpoint, minio_access_key, minio_secret_key)
        if news_data is None:
            logger.warning(f"No data for {ticker}, skipping")
            return
        
        # Step 2: Extract headlines
        headlines = extract_headlines(news_data)
        if not headlines:
            logger.warning(f"No headlines found for {ticker}")
            return
        
        # Step 3: Analyze sentiment (batch processing)
        sentiment_scores = analyzer.analyze_batch(headlines)
        
        # Step 4: Store in PostgreSQL
        store_sentiment_scores(ticker, headlines, sentiment_scores, db_connection_string)
        
        logger.info(f"Successfully processed sentiment for {ticker}")
        
    except Exception as e:
        logger.error(f"Error processing sentiment for {ticker}: {str(e)}")
        raise

def main(tickers, minio_endpoint, minio_access_key, minio_secret_key, db_connection_string):
    """
    Main entry point. Process sentiment for all tickers.
    
    This is what Airflow DAG will call.
    """
    # Load FinBERT once (reused for all tickers)
    analyzer = FinBERTAnalyzer()
    
    # Process each ticker
    for ticker in tickers:
        process_sentiment_for_ticker(
            ticker,
            analyzer,
            minio_endpoint,
            minio_access_key,
            minio_secret_key,
            db_connection_string
        )

if __name__ == "__main__":
    import os
    
    # Configuration
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    minio_access_key = os.getenv('MINIO_ROOT_USER', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
    db_connection_string = 'postgresql://market_sentinel:market_sentinel_password@localhost:5432/market_sentinel'
    
    # Run
    main(tickers, minio_endpoint, minio_access_key, minio_secret_key, db_connection_string)
