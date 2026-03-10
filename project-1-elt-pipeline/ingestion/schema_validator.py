"""
ingestion/schema_validator.py
------------------------------
Great Expectations checkpoint that validates raw.transactions
after ingestion. Runs as a standalone script or is called from
the Airflow DAG after the ingestion task completes.
"""

import logging
import os

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

POSTGRES_CONN = os.environ.get(
    "POSTGRES_DWH_CONN",
    "postgresql+psycopg2://fraud_user:fraud_pass@localhost:5433/fraud_db",
)


def build_expectations(validator):
    """Define the expectation suite for raw.transactions."""

    # Row count: dataset has 284,807 rows
    validator.expect_table_row_count_to_be_between(min_value=280_000, max_value=290_000)

    # No nulls on key columns
    for col in ["Time", "Amount", "Class"]:
        validator.expect_column_values_to_not_be_null(column=col)

    # Class is binary
    validator.expect_column_values_to_be_in_set(column="Class", value_set=[0, 1])

    # Amount is non-negative
    validator.expect_column_values_to_be_between(column="Amount", min_value=0)

    # Time is non-negative
    validator.expect_column_values_to_be_between(column="Time", min_value=0)

    # PCA features should be numeric (not null)
    for i in range(1, 29):
        validator.expect_column_values_to_not_be_null(column=f"V{i}")

    validator.save_expectation_suite(discard_failed_expectations=False)


def run_validation() -> bool:
    context = gx.get_context()

    datasource_config = {
        "name": "fraud_postgres",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": POSTGRES_CONN,
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["run_id"],
            }
        },
    }
    context.add_datasource(**datasource_config)

    suite_name = "raw_transactions_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name)

    batch_request = RuntimeBatchRequest(
        datasource_name="fraud_postgres",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="raw.transactions",
        runtime_parameters={"query": 'SELECT * FROM raw."transactions"'},
        batch_identifiers={"run_id": "latest"},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    build_expectations(validator)

    checkpoint_result = context.run_checkpoint(
        checkpoint_name="fraud_checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }
        ],
    )

    success = checkpoint_result["success"]
    if success:
        logger.info("Great Expectations validation PASSED")
    else:
        logger.error("Great Expectations validation FAILED — check GE docs for details")

    return success


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ok = run_validation()
    raise SystemExit(0 if ok else 1)
