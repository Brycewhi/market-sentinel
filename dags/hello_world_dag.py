from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments (settings applied to all tasks)
default_args = {
    'owner': 'market_sentinel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval='@daily',  # Run once per day
    start_date=days_ago(1),
    catchup=False,
    tags=['tutorial'],
) as dag:
    
    def print_hello():
        print("Hello from Airflow!")
        return "Hello World"
    
    def print_goodbye():
        print("Goodbye from Airflow!")
        return "Goodbye World"
    
    # Create tasks
    task_1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )
    
    task_2 = PythonOperator(
        task_id='print_goodbye',
        python_callable=print_goodbye,
    )
    
    # Define dependency: task_1 must complete before task_2 starts
    task_1 >> task_2
