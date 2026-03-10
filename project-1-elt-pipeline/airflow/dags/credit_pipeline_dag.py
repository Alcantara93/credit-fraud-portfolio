"""
credit_pipeline_dag.py
----------------------
Daily ELT pipeline for the Credit Card Fraud Detection dataset.

Task order:
    ingest_raw_data
        → validate_schema
            → dbt_run
                → dbt_test
                    → dbt_generate_docs

Airflow UI:  http://localhost:8080  (admin / admin)
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Make ingestion module importable from /opt/airflow/ingestion
sys.path.insert(0, "/opt/airflow")

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def run_ingestion(**context):
    from ingestion.ingest import run

    row_count = run()
    context["ti"].xcom_push(key="row_count", value=row_count)
    return row_count


def run_ge_validation(**context):
    row_count = context["ti"].xcom_pull(task_ids="ingest_raw_data", key="row_count")
    print(f"Validating {row_count:,} ingested rows with Great Expectations...")

    # Import here so missing GE install doesn't break DAG parsing
    from ingestion.schema_validator import run_validation

    success = run_validation()
    if not success:
        raise ValueError("Great Expectations validation failed — pipeline halted.")
    return success


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="credit_fraud_pipeline",
    default_args=default_args,
    description="Credit Card Fraud ELT: ingest → validate → dbt run → dbt test → docs",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["fraud", "elt", "finance", "dbt"],
) as dag:

    # 1 — Python ingestion
    ingest = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=run_ingestion,
    )

    # 2 — Great Expectations checkpoint
    validate = PythonOperator(
        task_id="validate_schema",
        python_callable=run_ge_validation,
    )

    # 3 — dbt run (all models)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # 4 — dbt test (schema + singular tests)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # 5 — dbt docs generate
    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt docs generate --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # Pipeline order
    ingest >> validate >> dbt_run >> dbt_test >> dbt_docs
