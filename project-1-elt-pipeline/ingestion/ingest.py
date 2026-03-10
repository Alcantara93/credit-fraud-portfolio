"""
ingestion/ingest.py
-------------------
Reads creditcard.csv, validates the schema, and loads it into
raw.transactions in the PostgreSQL data warehouse.

Environment variables:
    DATA_PATH          Path to creditcard.csv  (default: /opt/airflow/data/creditcard.csv)
    POSTGRES_DWH_CONN  SQLAlchemy connection string
"""

import logging
import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

DATA_PATH = os.environ.get("DATA_PATH", "/opt/airflow/data/creditcard.csv")
POSTGRES_CONN = os.environ.get(
    "POSTGRES_DWH_CONN",
    "postgresql+psycopg2://fraud_user:fraud_pass@localhost:5433/fraud_db",
)

EXPECTED_COLUMNS = ["Time", "Amount", "Class"] + [f"V{i}" for i in range(1, 29)]


# ---------------------------------------------------------------------------
# Step 1 — Load
# ---------------------------------------------------------------------------
def load_data(path: str) -> pd.DataFrame:
    logger.info("Loading data from %s", path)
    df = pd.read_csv(path)
    logger.info("Loaded %s rows × %s columns", f"{len(df):,}", len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Step 2 — Validate (lightweight pre-GE check)
# ---------------------------------------------------------------------------
def validate_schema(df: pd.DataFrame) -> None:
    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        logger.warning("Null values detected:\n%s", null_counts[null_counts > 0])

    if df["Class"].isin([0, 1]).all() is False:
        raise ValueError("Column 'Class' contains values other than 0/1")

    fraud_rate = df["Class"].mean() * 100
    logger.info("Fraud rate: %.4f%%", fraud_rate)
    logger.info("Schema validation passed")


# ---------------------------------------------------------------------------
# Step 3 — Create table (idempotent DDL)
# ---------------------------------------------------------------------------
CREATE_TABLE_SQL = text("""
    CREATE TABLE IF NOT EXISTS raw.transactions (
        "Time"   DOUBLE PRECISION,
        "V1"     DOUBLE PRECISION, "V2"  DOUBLE PRECISION, "V3"  DOUBLE PRECISION,
        "V4"     DOUBLE PRECISION, "V5"  DOUBLE PRECISION, "V6"  DOUBLE PRECISION,
        "V7"     DOUBLE PRECISION, "V8"  DOUBLE PRECISION, "V9"  DOUBLE PRECISION,
        "V10"    DOUBLE PRECISION, "V11" DOUBLE PRECISION, "V12" DOUBLE PRECISION,
        "V13"    DOUBLE PRECISION, "V14" DOUBLE PRECISION, "V15" DOUBLE PRECISION,
        "V16"    DOUBLE PRECISION, "V17" DOUBLE PRECISION, "V18" DOUBLE PRECISION,
        "V19"    DOUBLE PRECISION, "V20" DOUBLE PRECISION, "V21" DOUBLE PRECISION,
        "V22"    DOUBLE PRECISION, "V23" DOUBLE PRECISION, "V24" DOUBLE PRECISION,
        "V25"    DOUBLE PRECISION, "V26" DOUBLE PRECISION, "V27" DOUBLE PRECISION,
        "V28"    DOUBLE PRECISION,
        "Amount" DOUBLE PRECISION,
        "Class"  SMALLINT,
        ingested_at TIMESTAMP DEFAULT NOW()
    )
""")


# ---------------------------------------------------------------------------
# Step 4 — Load into Postgres
# ---------------------------------------------------------------------------
def ingest_to_postgres(df: pd.DataFrame, engine) -> int:
    df = df.copy()
    df["ingested_at"] = datetime.utcnow()

    with engine.begin() as conn:
        conn.execute(CREATE_TABLE_SQL)
        conn.execute(text("TRUNCATE TABLE raw.transactions"))

    row_count = len(df)
    df.to_sql(
        name="transactions",
        con=engine,
        schema="raw",
        if_exists="append",
        index=False,
        chunksize=10_000,
        method="multi",
    )
    logger.info("Ingested %s rows into raw.transactions", f"{row_count:,}")
    return row_count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run() -> int:
    df = load_data(DATA_PATH)
    validate_schema(df)
    engine = create_engine(POSTGRES_CONN)
    return ingest_to_postgres(df, engine)


if __name__ == "__main__":
    total = run()
    logger.info("Done. %s rows loaded.", f"{total:,}")
