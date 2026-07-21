from airflow.decorators import task, dag
from datetime import datetime, timedelta

default_args = {
    "owner": "Souptik",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="taskflow_dag",
    description="A simple DAG to demonstrate TaskFlow API",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",  # schedule_interval is deprecated in Airflow 2.4+
    default_args=default_args,
    catchup=False,
)
def helloWorld():

    @task
    def name():
        return "Souptik"

    @task
    def age():
        return 22

    @task(multiple_outputs=True)
    def info():
        return {
            "name": "Souptik",
            "age": 22,
        }

    @task
    def greet(name, age):
        return f"Hello {name}, you are {age}!"

    name_task = name()
    age_task = age()
    info_task = info()

    greet_task = greet(name_task, age_task)

    greet_task2 = greet(
        name=info_task["name"],
        age=info_task["age"],
    )

hello_world_dag = helloWorld()