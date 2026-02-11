from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Add scripts directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from process_sentiment import main

default_args = {
    'owner': 'market_sentinel',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_sentiment',
    default_args=default_args,
    description='Hourly sentiment analysis with FinBERT',
    schedule_interval='0 23 * * *',  # 11 PM UTC daily (1 hour after news ingestion)
    start_date=days_ago(1),
    catchup=False,
    tags=['data_pipeline', 'sentiment_analysis'],
) as dag:
    
    # Configuration
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    minio_endpoint = 'http://minio:9000'  # Inside Docker network
    minio_access_key = os.getenv('MINIO_ROOT_USER', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
    db_connection_string = 'postgresql://market_sentinel:market_sentinel_password@postgres:5432/market_sentinel'
    
    # Single task that processes all tickers
    # (FinBERT is loaded once, reused for all tickers = efficient)
    analyze_sentiment = PythonOperator(
        task_id='analyze_sentiment',
        python_callable=main,
        op_kwargs={
            'tickers': tickers,
            'minio_endpoint': minio_endpoint,
            'minio_access_key': minio_access_key,
            'minio_secret_key': minio_secret_key,
            'db_connection_string': db_connection_string,
        },
    )
