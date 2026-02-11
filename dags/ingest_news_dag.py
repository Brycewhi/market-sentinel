from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Add scripts directory to Python path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from fetch_news import fetch_and_store_news

# Define default arguments (settings applied to all tasks)
default_args = {
    'owner': 'market_sentinel',
    'retries': 3,  # Retry 3 times if a task fails
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Define the DAG
with DAG(
    'ingest_news',
    default_args=default_args,
    description='Hourly news ingestion from Alpha Vantage to MinIO',
    schedule_interval='0 22 * * *',  # 10 PM UTC daily
    start_date=days_ago(1),
    catchup=False,  # Don't backfill old dates
    tags=['data_pipeline', 'news_ingestion'],
) as dag:
    
    # Get environment variables (set in .env, passed to Airflow)
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    minio_endpoint = 'http://minio:9000'  # Using Docker service name
    minio_access_key = os.getenv('MINIO_ROOT_USER', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
    
    # Create tasks for each ticker
    # We'll fetch news for AAPL, MSFT, and GOOGL
    # Each runs in parallel (no dependencies between them)
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    
    for ticker in tickers:
        task = PythonOperator(
            task_id=f'fetch_news_{ticker}',
            python_callable=fetch_and_store_news,
            op_kwargs={
                'ticker': ticker,
                'api_key': api_key,
                'minio_endpoint': minio_endpoint,
                'minio_access_key': minio_access_key,
                'minio_secret_key': minio_secret_key,
            },
        )
