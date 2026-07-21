from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator


AWS_CONN_ID = "aws_default"
LOCAL_FILE_PATH = "/opt/airflow/data/data.csv"
S3_BUCKET = "airflow"
S3_KEY = "airflow-demo/data.csv"


default_args = {
    "owner": "Souptik",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def upload_data_csv_to_s3():
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    s3_hook.load_file(
        filename=LOCAL_FILE_PATH,
        key=S3_KEY,
        bucket_name=S3_BUCKET,
        replace=True,
    )

    return f"Uploaded {LOCAL_FILE_PATH} to s3://{S3_BUCKET}/{S3_KEY}"


with DAG(
    dag_id="s3_upload_data_csv",
    description="Upload data.csv from the local Airflow data folder to S3.",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["s3", "aws", "csv"],
) as dag:
    upload_data_csv = PythonOperator(
        task_id="upload_data_csv_to_s3",
        python_callable=upload_data_csv_to_s3,
    )
