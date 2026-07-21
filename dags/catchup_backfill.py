from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Souptik',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# catchup is set to False, so the DAG will not run for any past dates when it is first deployed. It will only run for the current date and future dates.
#backfill is the process of running a DAG for past dates. If catchup is set to True, the DAG will run for all past dates from the start_date to the current date when it is first deployed. If catchup is set to False, the DAG will only run for the current date and future dates, and any past dates will be skipped.

with DAG(
    dag_id="catchup_backfill_dagv1",
    description="A simple DAG to demonstrate catchup and backfill",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
)
as dag:
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