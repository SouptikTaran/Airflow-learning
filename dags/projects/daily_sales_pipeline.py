import csv
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


PROJECT_DIR = "/opt/airflow/projects/daily_sales_pipeline"
RAW_SALES_CSV = f"{PROJECT_DIR}/data/sales.csv"
CLEAN_SALES_CSV = f"{PROJECT_DIR}/data/sales_clean.csv"
SUMMARY_REPORT_CSV = f"{PROJECT_DIR}/reports/sales_summary.csv"
CREATE_TABLES_SQL = f"{PROJECT_DIR}/sql/create_tables.sql"
REFRESH_SUMMARY_SQL = f"{PROJECT_DIR}/sql/refresh_summary.sql"

REQUIRED_COLUMNS = [
    "order_id",
    "order_date",
    "customer",
    "region",
    "product",
    "quantity",
    "unit_price",
]


default_args = {
    "owner": "Souptik",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_postgres_connection():
    return psycopg2.connect(
        host="postgres-new",
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow",
    )


def check_input_file():
    if not os.path.exists(RAW_SALES_CSV):
        raise FileNotFoundError(f"Missing input file: {RAW_SALES_CSV}")
    return RAW_SALES_CSV


def validate_sales_csv():
    with open(RAW_SALES_CSV, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        missing_columns = set(REQUIRED_COLUMNS) - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(f"Missing columns: {sorted(missing_columns)}")

        row_count = 0
        for row in reader:
            row_count += 1
            int(row["order_id"])
            datetime.strptime(row["order_date"], "%Y-%m-%d")
            quantity = int(row["quantity"])
            unit_price = float(row["unit_price"])
            if quantity <= 0:
                raise ValueError(f"Invalid quantity in row {row_count}")
            if unit_price < 0:
                raise ValueError(f"Invalid unit price in row {row_count}")

    if row_count == 0:
        raise ValueError("Input CSV has no data rows")

    return row_count


def transform_sales_csv():
    os.makedirs(os.path.dirname(CLEAN_SALES_CSV), exist_ok=True)

    with open(RAW_SALES_CSV, newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        output_columns = REQUIRED_COLUMNS + ["total_amount"]

        with open(CLEAN_SALES_CSV, "w", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=output_columns)
            writer.writeheader()

            for row in reader:
                quantity = int(row["quantity"])
                unit_price = float(row["unit_price"])
                row["total_amount"] = f"{quantity * unit_price:.2f}"
                writer.writerow({column: row[column] for column in output_columns})

    return CLEAN_SALES_CSV


def run_sql_file(path):
    with open(path, encoding="utf-8") as file:
        sql = file.read()

    with get_postgres_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)


def create_tables():
    run_sql_file(CREATE_TABLES_SQL)


def load_sales_to_postgres():
    with get_postgres_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE demo_sales_orders;")

            with open(CLEAN_SALES_CSV, newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    cursor.execute(
                        """
                        INSERT INTO demo_sales_orders (
                            order_id,
                            order_date,
                            customer,
                            region,
                            product,
                            quantity,
                            unit_price,
                            total_amount
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                        (
                            int(row["order_id"]),
                            row["order_date"],
                            row["customer"],
                            row["region"],
                            row["product"],
                            int(row["quantity"]),
                            row["unit_price"],
                            row["total_amount"],
                        ),
                    )


def refresh_sales_summary():
    run_sql_file(REFRESH_SUMMARY_SQL)


def export_sales_summary_report():
    os.makedirs(os.path.dirname(SUMMARY_REPORT_CSV), exist_ok=True)

    with get_postgres_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT region, order_count, total_quantity, total_amount, refreshed_at
                FROM demo_sales_summary
                ORDER BY region;
                """
            )
            rows = cursor.fetchall()

    with open(SUMMARY_REPORT_CSV, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            ["region", "order_count", "total_quantity", "total_amount", "refreshed_at"]
        )
        writer.writerows(rows)

    return SUMMARY_REPORT_CSV


def upload_report_to_s3_if_configured():
    bucket = Variable.get("sales_report_s3_bucket", default_var="")
    key = Variable.get(
        "sales_report_s3_key",
        default_var="airflow-demo/sales_summary.csv",
    )

    if not bucket:
        raise AirflowSkipException("Set Airflow Variable sales_report_s3_bucket to upload to S3")

    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file(
        filename=SUMMARY_REPORT_CSV,
        key=key,
        bucket_name=bucket,
        replace=True,
    )

    return f"s3://{bucket}/{key}"


with DAG(
    dag_id="project_daily_sales_pipeline",
    description="Validate sales CSV, load Postgres, generate report, and optionally upload to S3.",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["project", "sales", "postgres", "s3"],
) as dag:
    check_input = PythonOperator(
        task_id="check_input_file",
        python_callable=check_input_file,
    )

    validate_csv = PythonOperator(
        task_id="validate_sales_csv",
        python_callable=validate_sales_csv,
    )

    transform_csv = PythonOperator(
        task_id="transform_sales_csv",
        python_callable=transform_sales_csv,
    )

    create_postgres_tables = PythonOperator(
        task_id="create_postgres_tables",
        python_callable=create_tables,
    )

    load_to_postgres = PythonOperator(
        task_id="load_sales_to_postgres",
        python_callable=load_sales_to_postgres,
    )

    refresh_summary = PythonOperator(
        task_id="refresh_sales_summary",
        python_callable=refresh_sales_summary,
    )

    export_report = PythonOperator(
        task_id="export_sales_summary_report",
        python_callable=export_sales_summary_report,
    )

    upload_s3 = PythonOperator(
        task_id="upload_report_to_s3_if_configured",
        python_callable=upload_report_to_s3_if_configured,
    )

    (
        check_input
        >> validate_csv
        >> transform_csv
        >> create_postgres_tables
        >> load_to_postgres
        >> refresh_summary
        >> export_report
        >> upload_s3
    )
