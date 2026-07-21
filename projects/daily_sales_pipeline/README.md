# Daily Sales Pipeline

This demo shows a realistic Airflow workflow:

1. Check that the input CSV exists.
2. Validate required columns and numeric values.
3. Transform rows into a clean CSV.
4. Create Postgres tables.
5. Load cleaned rows into Postgres.
6. Generate a sales summary report.
7. Optionally upload the report to S3.

## Airflow DAG

The DAG is defined at:

```text
dags/projects/daily_sales_pipeline.py
```

DAG ID:

```text
project_daily_sales_pipeline
```

## Optional S3 Upload

The S3 task is skipped unless you set Airflow Variables:

```text
sales_report_s3_bucket
sales_report_s3_key
```

You also need an Airflow AWS connection named:

```text
aws_default
```

