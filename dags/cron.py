from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Souptik',
    'retries': 1,
    retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="cron_dag",
    description="A simple DAG to demonstrate cron scheduling",
    start_date=datetime(2026, 1, 1),
    # schedule_interval="0 12 * * *",  # This is a cron expression for running the DAG at 12:00 PM every day
    schedule_interval=@interval,  # @daily , @hourly, @weekly, @monthly, @yearly, @once
    catchup=False,
    default_args=default_args
) as dag:
    task1 = BashOperator(
        task_id="Hello_world", 
        bash_command="echo 'Hello World!'"
    )

    task2 = BashOperator(
        task_id="Goodbye_world",
        bash_command="echo 'Goodbye World!'"
    )

    task3 = BashOperator(
        task_id="Hello_Airflow",
        bash_command="echo 'Hello Airflow!'"
    )

    task1 >> [task2, task3]