from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Souptik',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="our_first_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
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

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    # Task dependency method 3
    task1 >> [task2, task3]
