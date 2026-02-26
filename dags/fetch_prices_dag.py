"""
fetch_prices_dag.py - Fetch daily stock prices using yfinance

This DAG runs daily at 6 PM UTC (after market close) to fetch
stock price data for AAPL, MSFT, and GOOGL.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts directory to Python path so we can import our module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from fetch_prices import main as fetch_prices_main

# Default arguments for all tasks
default_args = {
    'owner': 'market_sentinel',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'fetch_prices',
    default_args=default_args,
    description='Fetch daily stock prices from Yahoo Finance',
    schedule_interval='0 22 * * *',  # Run at 10 PM UTC daily (after US market close)
    start_date=datetime(2026, 2, 1),
    catchup=False,  # Don't backfill historical dates
    tags=['data_pipeline', 'price_data'],
) as dag:
    
    # Task: Fetch and store price data
    fetch_prices_task = PythonOperator(
        task_id='fetch_and_store_prices',
        python_callable=fetch_prices_main,
    )

    # Simple DAG with just one task
    fetch_prices_task