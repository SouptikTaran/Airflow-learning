# Airflow Demo Projects

This folder contains self-contained demo projects for learning Airflow.

Each project keeps its own data, SQL, generated reports, and notes. The DAG
entrypoints live under `dags/projects/` so Airflow can discover them.

## Projects

- `daily_sales_pipeline` - validates a CSV file, transforms rows, loads them
  into Postgres, creates a sales summary report, and can upload that report to
  S3 when configured.

