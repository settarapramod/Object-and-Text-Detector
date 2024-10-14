from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 10, 14),
    catchup=False,
) as dag:
    
    # Define the task function
    def say_hello():
        print("Hello, World!")

    # Create a PythonOperator task
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=say_hello,
    )

    # Set task dependencies (in this case, just one task)
    hello_task
