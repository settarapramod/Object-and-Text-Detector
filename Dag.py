from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'workflow_dag',
    default_args=default_args,
    description='DAG to run Python workflow with command-line parameters',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
) as dag:

    # Bash task to run the Python script with parameters
    run_workflow = BashOperator(
        task_id='run_python_workflow',
        bash_command='python /home/airflow/gcs/data/workflow.py {{ dag_run.conf["process_type"] }} {{ dag_run.conf["process_id"] }}',
    )
