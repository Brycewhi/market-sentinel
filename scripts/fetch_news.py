import requests
import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import os
import logging

# Setup logging so we can see what's happening
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_news_from_alpha_vantage(ticker, api_key):
    """
    Fetch news and sentiment from Alpha Vantage API.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL')
        api_key: Your Alpha Vantage API key
    
    Returns:
        dict: Raw API response containing news articles
    
    Raises:
        Exception: If API returns an error
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": ticker,
        "apikey": api_key,
        "limit": 50  # Get up to 50 articles per request
    }
    
    logger.info(f"Fetching news for {ticker} from Alpha Vantage...")
    response = requests.get(url, params=params)
    
    # Check for API errors
    if 'error message' in response.json():
        raise Exception(f"Alpha Vantage API error: {response.json()['error message']}")
    
    if 'Note' in response.json():
        raise Exception(f"Alpha Vantage rate limit hit: {response.json()['Note']}")
    
    logger.info(f"Successfully fetched news for {ticker}")
    return response.json()

def save_to_minio(data, ticker, minio_endpoint, access_key, secret_key):
    """
    Save JSON data to MinIO (S3-compatible storage).
    
    Args:
        data: Python dict to save as JSON
        ticker: Stock symbol (used in filename)
        minio_endpoint: MinIO endpoint (e.g., 'http://minio:9000')
        access_key: MinIO access key
        secret_key: MinIO secret key
    
    Returns:
        str: The filename that was saved
    """
    # Initialize S3 client pointed at MinIO
    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    
    # Create bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket='raw-news')
    except ClientError:
        logger.info("Creating raw-news bucket...")
        s3.create_bucket(Bucket='raw-news')
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{ticker}_{timestamp}.json"
    
    # Save file to MinIO
    s3.put_object(
        Bucket='raw-news',
        Key=filename,
        Body=json.dumps(data, indent=2)
    )
    
    logger.info(f"Saved to MinIO: s3://raw-news/{filename}")
    return filename

def fetch_and_store_news(ticker, api_key, minio_endpoint, minio_access_key, minio_secret_key):
    """
    Complete pipeline: fetch news from API, save to MinIO.
    
    This is the main function that Airflow will call.
    """
    try:
        # Step 1: Fetch from Alpha Vantage
        news_data = fetch_news_from_alpha_vantage(ticker, api_key)
        
        # Step 2: Save to MinIO
        filename = save_to_minio(
            data=news_data,
            ticker=ticker,
            minio_endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key
        )
        
        logger.info(f"Successfully fetched and stored news for {ticker}")
        return filename
        
    except Exception as e:
        logger.error(f"Error fetching/storing news for {ticker}: {str(e)}")
        raise

# For testing locally (optional)
if __name__ == "__main__":
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    minio_access_key = os.getenv('MINIO_ROOT_USER', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
    
    # Test with AAPL
    try:
        fetch_and_store_news('AAPL', api_key, minio_endpoint, minio_access_key, minio_secret_key)
        print("✅ Test successful!")
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
