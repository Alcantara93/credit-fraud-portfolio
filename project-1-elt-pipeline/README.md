# Project 1 — Credit Card ELT Pipeline

Production-grade ELT pipeline using **PostgreSQL · dbt Core · Apache Airflow · Great Expectations** — containerised with Docker Compose.

---

## Architecture

```
creditcard.csv
     │
     ▼
Python Ingestion (ingest.py)
     │  Great Expectations validation
     ▼
PostgreSQL  raw.transactions          ← Bronze
     │
     ▼  dbt staging model
PostgreSQL  silver.stg_transactions   ← Silver (view)
     │
     ▼  dbt intermediate model
PostgreSQL  silver.int_fraud_features ← Silver (table, feature engineering)
     │
     ├──▶  gold.fraud_mart            ← Gold (fact table + risk score)
     └──▶  gold.reporting_mart        ← Gold (hourly aggregates)

All steps orchestrated by Airflow DAG (daily schedule)
```

---

## Quick Start

### 1. Prerequisites
- Docker Desktop
- `creditcard.csv` copied to `../data/creditcard.csv`

### 2. Start the stack

```bash
cd project-1-elt-pipeline

# Copy env file
cp .env.example .env

# Start all services (first run takes ~5 min to pull images)
docker compose up -d

# Watch logs
docker compose logs -f airflow-scheduler
```

### 3. Trigger the pipeline

Open **http://localhost:8080** → login `admin / admin`

Find `credit_fraud_pipeline` → toggle ON → click **Trigger DAG**

### 4. Query the results

```bash
# Connect to the DWH
docker exec -it fraud_dwh psql -U fraud_user -d fraud_db

-- Check Gold layer
SELECT * FROM gold.reporting_mart ORDER BY transaction_hour LIMIT 10;
SELECT risk_score, COUNT(*) FROM gold.fraud_mart GROUP BY 1;
```

---

## Project Structure

```
project-1-elt-pipeline/
├── docker-compose.yml          # All services: Airflow + 2× PostgreSQL
├── init_db.sql                 # Schema + RBAC setup (runs on first boot)
├── .env.example                # Environment variable template
│
├── ingestion/
│   ├── ingest.py               # CSV → raw.transactions loader
│   ├── schema_validator.py     # Great Expectations checkpoint
│   └── requirements.txt
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml            # Uses env vars — no hardcoded credentials
│   ├── packages.yml            # dbt_utils
│   ├── macros/
│   │   └── amount_bucket.sql   # Reusable spend-tier macro
│   ├── tests/
│   │   └── assert_fraud_rate_below_5pct.sql
│   └── models/
│       ├── staging/            # Bronze → Silver views
│       ├── intermediate/       # Feature engineering tables
│       └── marts/              # Gold fact + reporting tables
│
└── airflow/
    └── dags/
        └── credit_pipeline_dag.py
```

---

## dbt Lineage

```
raw.transactions (source)
    └── stg_transactions (view)
            └── int_fraud_features (table)
                    ├── fraud_mart (table)
                    └── reporting_mart (table)
```

---

## Senior-Level Design Decisions

| Decision | Rationale |
|---|---|
| Medallion Architecture (raw / silver / gold) | Mirrors enterprise lakehouse patterns; each layer has a clear contract |
| RBAC roles (raw_loader, transformer, reporter) | Principle of least privilege; mirrors Snowflake RBAC for portability |
| dbt env-var profiles | Zero hardcoded credentials; works locally and in CI identically |
| Great Expectations validation | Data quality gate before transformations run |
| XCom row_count propagation | Observability — scheduler knows how many rows were loaded |
| Rule-based risk_score | Explainable for compliance / FINMA audit requirements |
