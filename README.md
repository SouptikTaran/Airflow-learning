# Atrium Airflow Workspace

Atrium is a local Apache Airflow learning workspace. It contains example DAGs,
sample data, and a more complete demo project that validates sales data, loads
it into Postgres, generates a summary report, and can optionally publish that
report to S3.

The repo is designed to run Airflow with Docker Compose using a separate Compose
project name, host port, and Postgres volume so it can coexist with another
Airflow stack on the same machine.

## Repository Layout

```text
.
|-- dags/                         # Airflow DAG definitions
|   `-- projects/                 # Project DAG entrypoints
|-- data/                         # Shared local data mounted into Airflow
|-- projects/                     # Self-contained demo projects
|   `-- daily_sales_pipeline/     # Sales CSV -> Postgres -> report pipeline
|-- config/                       # Airflow config mount
|-- plugins/                      # Airflow plugin mount
|-- logs/                         # Airflow logs, ignored by git
|-- docker-compose.yaml           # Local Airflow + Postgres stack
`-- .env                          # Local Airflow UID
```

## Requirements

- Docker
- Docker Compose

The Compose file uses the `apache/airflow:2.10.5` image by default and runs
Airflow with `LocalExecutor` backed by Postgres 13.

## Quick Start

Start the Airflow stack:

```powershell
docker compose up airflow-init-new
docker compose up
```

Open the Airflow UI:

```text
http://localhost:8081
```

Default login:

```text
username: airflow
password: airflow
```

Stop the stack:

```powershell
docker compose down
```

Stop the stack and remove the local Postgres volume:

```powershell
docker compose down --volumes
```

## Main DAGs

- `project_daily_sales_pipeline` - validates `projects/daily_sales_pipeline/data/sales.csv`,
  transforms it, loads it into Postgres, refreshes summary tables, exports
  `projects/daily_sales_pipeline/reports/sales_summary.csv`, and optionally
  uploads the report to S3.
- `s3_upload_data_csv` - uploads `data/data.csv` to S3 using the `aws_default`
  Airflow connection.
- `dag_with_postgres_operator_v03` - demonstrates `PostgresOperator` usage.
- `dag_with_postgres_hooks_v04` - demonstrates extracting rows from Postgres
  and uploading the result to S3.
- `taskflow_dag`, `python_dag_v1`, `our_first_dag`, and
  `catchup_backfill_dagv1` - smaller examples for TaskFlow, Python operators,
  Bash operators, dependencies, retries, catchup, and scheduling.

Note: `dags/cron.py` currently contains scratch/example syntax and should be
fixed or removed before expecting Airflow to parse every DAG without import
errors.

## Daily Sales Pipeline

The most complete project is documented in
`projects/daily_sales_pipeline/README.md`.

Its DAG file is:

```text
dags/projects/daily_sales_pipeline.py
```

The pipeline uses the Postgres service from this Compose stack directly:

```text
host: postgres-new
database: airflow
user: airflow
password: airflow
```

Generated files are written under:

```text
projects/daily_sales_pipeline/data/sales_clean.csv
projects/daily_sales_pipeline/reports/sales_summary.csv
```

## Optional Connections and Variables

Some DAGs require Airflow connections or variables before they can run
successfully.

For `project_daily_sales_pipeline`, S3 upload is skipped unless this variable is
set:

```text
sales_report_s3_bucket
```

Optional S3 key variable:

```text
sales_report_s3_key
```

The S3 upload task expects an Airflow AWS connection named:

```text
aws_default
```

The Postgres example DAGs expect a Postgres connection named:

```text
postgres_localhost
```

The Postgres-to-S3 hook example expects an S3-compatible connection named:

```text
minio_conn
```

## Useful Commands

List DAGs from the Airflow container:

```powershell
docker compose run --rm airflow-cli-new dags list
```

Test a task:

```powershell
docker compose run --rm airflow-cli-new tasks test project_daily_sales_pipeline validate_sales_csv 2026-01-01
```

Trigger the sales pipeline:

```powershell
docker compose run --rm airflow-cli-new dags trigger project_daily_sales_pipeline
```

Open an Airflow shell command:

```powershell
docker compose run --rm airflow-cli-new bash
```

## Git Notes

Runtime files are intentionally ignored:

```text
.env
logs/*
dags/__pycache__/
```

Keep generated logs and local environment values out of commits.
