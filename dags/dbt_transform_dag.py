"""
dbt_transform_dag.py - Run dbt transformations after sentiment processing
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'market_sentinel',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_transform',
    default_args=default_args,
    description='Run dbt transformations on sentiment data',
    schedule_interval='30 23 * * *',  # 11:30 PM UTC (30 min after sentiment processing)
    catchup=False,
    tags=['dbt', 'transformation'],
) as dag:

    # Run all dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/market_sentinel && dbt run',
    )

    # Run all dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/market_sentinel && dbt test',
    )

    # Set dependencies
    dbt_run >> dbt_test  # Run tests after models succeed
