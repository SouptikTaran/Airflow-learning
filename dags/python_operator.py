from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def greet(age, ti):
    name = ti.xcom_pull(task_ids="get_name_task")
    return f"Hello {name}, you are {age}!"

def greetv2(ti):
    name = ti.xcom_pull(task_ids="get_name_task")
    age = ti.xcom_pull(task_ids="push_name_task", key="age")
    return f"Hello {name}, you are {age}! from v2"

def getName():
    return "Souptik"

def pushName(ti):
    ti.xcom_push(key="name", value="Souptik")
    ti.xcom_push(key="age", value=22)

default_args = {
    'owner': 'Souptik',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="python_dag_v1",
    description="A simple DAG to demonstrate python operators",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:
    task1 = PythonOperator(
        task_id="greet_task",
        python_callable=greet,
        op_kwargs={'age': 22}
    )
    task2 = PythonOperator(
        task_id="get_name_task",
        python_callable=getName
    )

    task2 >> task1

    task3 = PythonOperator(
        task_id="push_name_task",
        python_callable=pushName
    )

    task4 = PythonOperator(
        task_id="greet_task_v2",
        python_callable=greetv2
    )

    task3 >> task4