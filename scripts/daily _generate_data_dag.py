from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import sys
import os

# Add DAGs directory to Python path
sys.path.append(os.path.dirname(__file__))

# Import function from generate_data.py
from generate_data import generate_data

# Define IST timezone
IST = pendulum.timezone("Asia/Kolkata")

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 10, tzinfo=IST),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "daily_generate_data",
    default_args=default_args,
    description="DAG to run generate_data daily at 10 AM IST",
    schedule_interval="0 4 * * *",  # Runs at 4 AM UTC (which is 10 AM IST)
    catchup=False,
)

# Define the Python Operator task
generate_data_task = PythonOperator(
    task_id="run_generate_data",
    python_callable=generate_data,
    dag=dag,
)

# Set task dependencies
generate_data_task
