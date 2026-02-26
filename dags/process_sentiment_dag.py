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
    
    # Create a task for each ticker
    for ticker in tickers:
        analyze_sentiment_task = PythonOperator(
            task_id=f'analyze_sentiment_{ticker}',
            python_callable=main,
            op_kwargs={
                'ticker': ticker,
            },
        )
    
