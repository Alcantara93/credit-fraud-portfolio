# Credit Card Fraud Detection — Data Engineering Portfolio

> **Enterprise-grade ELT pipeline** built on real financial data, demonstrating the full stack a Senior Data Engineer uses in production: ingestion, validation, transformation, orchestration, and CI/CD — all containerised and reproducible.

---

## What This Project Does

Banks and FinTech companies process millions of card transactions every day. A tiny fraction are fraud. This project builds the **data infrastructure** that would allow a fraud investigation team to:

- Automatically ingest and validate raw transaction data every day
- Transform it through clearly defined quality layers (Bronze → Silver → Gold)
- Produce analytics-ready tables with engineered features and explainable risk scores
- Run automated data quality tests on every pipeline run
- View full data lineage documentation

**The dataset:** 284,807 real anonymised credit card transactions from European cardholders, with 492 confirmed fraudulent cases (0.172% fraud rate). Source: [Kaggle Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud).

---

## Architecture Overview

```
creditcard.csv  (284,807 rows)
       │
       ▼
 ┌─────────────────────────────────────────┐
 │  Python Ingestion  (ingest.py)          │
 │  · Loads CSV into PostgreSQL            │
 │  · Great Expectations schema validation │
 └──────────────────┬──────────────────────┘
                    │
                    ▼  raw.transactions
 ┌─────────────────────────────────────────┐
 │  BRONZE LAYER  (raw schema)             │
 │  Exact copy of source data + timestamp  │
 └──────────────────┬──────────────────────┘
                    │
                    ▼  dbt staging model
 ┌─────────────────────────────────────────┐
 │  SILVER LAYER  (silver schema)          │
 │  · stg_transactions  — cleaned view     │
 │  · int_fraud_features — engineered      │
 │    features (transaction hour,          │
 │    spend tier, 1-hour velocity)         │
 └──────────────────┬──────────────────────┘
                    │
          ┌─────────┴──────────┐
          ▼                    ▼  dbt mart models
 ┌────────────────┐  ┌──────────────────────┐
 │  GOLD LAYER    │  │  GOLD LAYER          │
 │  fraud_mart    │  │  reporting_mart      │
 │  · Fact table  │  │  · Hourly aggregates │
 │  · Risk score  │  │  · Fraud rate by     │
 │    (LOW/MED/   │  │    hour & amount     │
 │     HIGH)      │  │    bucket            │
 └────────────────┘  └──────────────────────┘

All steps orchestrated by Apache Airflow (daily schedule)
```

---

## Tech Stack

| Tool | Role |
|---|---|
| **Python + Pandas** | Data ingestion, schema validation |
| **PostgreSQL 15** | Data warehouse (Bronze / Silver / Gold schemas) |
| **dbt Core** | SQL transformations, testing, documentation |
| **Apache Airflow 2.8** | Pipeline orchestration (daily DAG) |
| **Great Expectations** | Data quality gates before transformations |
| **Docker Compose** | One-command local deployment of full stack |
| **GitHub Actions** | CI — dbt compile check on every pull request |

---

## Project Structure

```
credit-fraud-portfolio/
│
├── data/                          ← Place creditcard.csv here (gitignored)
│
└── project-1-elt-pipeline/
    ├── docker-compose.yml         # Full stack: Airflow + 2× PostgreSQL
    ├── init_db.sql                # Schema creation + RBAC roles
    ├── .env.example               # Environment variable template
    │
    ├── ingestion/
    │   ├── ingest.py              # CSV → raw.transactions loader
    │   ├── schema_validator.py    # Great Expectations checkpoint
    │   └── requirements.txt
    │
    ├── dbt/
    │   ├── models/
    │   │   ├── staging/           # Bronze → Silver (views)
    │   │   ├── intermediate/      # Feature engineering (tables)
    │   │   └── marts/             # Gold fact + reporting tables
    │   ├── macros/
    │   │   └── amount_bucket.sql  # Reusable spend-tier macro
    │   └── tests/
    │       └── assert_fraud_rate_below_5pct.sql
    │
    └── airflow/
        └── dags/
            └── credit_pipeline_dag.py
```

---

## Quick Start (5 minutes)

### Prerequisites
- Docker Desktop running
- `creditcard.csv` downloaded from [Kaggle](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) and placed in `data/`

### 1. Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/credit-fraud-portfolio.git
cd credit-fraud-portfolio/project-1-elt-pipeline
cp .env.example .env
```

### 2. Start the full stack

```bash
docker compose up -d
# First run pulls images (~5 min). Watch progress:
docker compose logs -f airflow-scheduler
```

### 3. Trigger the pipeline

Open **http://localhost:8080** → login `admin / admin`

Find `credit_fraud_pipeline` → toggle ON → click **Trigger DAG**

The DAG runs these steps in order:

```
ingest_raw_data → validate_schema → dbt_run → dbt_test → dbt_generate_docs
```

### 4. Query the results

```bash
docker exec -it fraud_dwh psql -U fraud_user -d fraud_db
```

```sql
-- Fraud breakdown by risk score
SELECT risk_score, COUNT(*) AS transactions, SUM(is_fraud::int) AS fraud_cases
FROM gold.fraud_mart
GROUP BY risk_score
ORDER BY 1;

-- Hourly fraud rate
SELECT transaction_hour, total_transactions, fraud_transactions, fraud_rate_pct
FROM gold.reporting_mart
ORDER BY transaction_hour;
```

---

## Key Engineering Decisions

### Medallion Architecture (Bronze / Silver / Gold)

Each data layer has a clear contract:

| Layer | Schema | Description |
|---|---|---|
| Bronze | `raw` | Exact copy of source data, append-only, never modified |
| Silver | `silver` | Cleaned, typed, feature-engineered — business logic lives here |
| Gold | `gold` | Analytics-ready aggregates and fact tables for consumers |

This mirrors how major data platforms (Databricks, Snowflake) organise data at scale.

### RBAC — Principle of Least Privilege

Three database roles are created at startup:

- `raw_loader` — can only write to the Bronze layer
- `transformer` — can read Bronze, write Silver and Gold
- `reporter` — read-only on Gold

This pattern is directly portable to Snowflake and BigQuery RBAC and satisfies audit requirements in regulated industries (banking, insurance).

### Data Quality Gates with Great Expectations

Before dbt runs a single transformation, Great Expectations checks that:
- All 31 expected columns are present
- `Class` contains only `0` (legitimate) or `1` (fraud)
- No unexpected nulls exist

If validation fails, the pipeline halts immediately — bad data never reaches the Silver or Gold layers.

### Explainable Risk Score

The `fraud_mart` Gold table includes a rule-based `risk_score` field:

```sql
CASE
    WHEN velocity_1h > 10 AND transaction_amount > 500 THEN 'HIGH'
    WHEN velocity_1h > 5  OR  transaction_amount > 200 THEN 'MEDIUM'
    ELSE 'LOW'
END AS risk_score
```

Rule-based scoring is intentional. In regulated financial environments (FINMA in Switzerland, FCA in the UK), black-box ML scores can block compliance sign-off. A transparent rule is auditable and explainable to regulators.

### Zero Hardcoded Credentials

dbt profiles and the ingestion script read all credentials from environment variables. The `.env` file is gitignored. This is the standard approach in CI/CD pipelines and cloud deployments.

### Airflow XCom Observability

The ingestion task pushes `row_count` via XCom, so downstream tasks (and operators) can verify the expected volume was loaded. This is a simple but important pattern for pipeline observability.

---

## What an Employer Can Take From This

| What You See | What It Signals |
|---|---|
| Medallion architecture with dbt | Knows enterprise lakehouse patterns; can ramp on Snowflake/Databricks immediately |
| RBAC roles in SQL | Security-conscious; understands governance at the database layer |
| Great Expectations integration | Treats data quality as a first-class citizen, not an afterthought |
| Docker Compose full-stack setup | Can build reproducible local environments; ready for Kubernetes migration |
| Airflow DAG with retries + XCom | Understands task dependencies, failure handling, and observability |
| GitHub Actions CI on dbt | Familiar with modern CI/CD for data projects |
| Rule-based risk score with compliance note | Understands the regulatory environment in FinTech and banking |
| Environment variable profiles | Follows security best practices; no secrets in code |

---

## Dataset

```
Rows:     284,807 transactions
Features: 30 PCA-anonymised features (V1–V28) + Time + Amount
Label:    Class (0 = legitimate, 1 = fraud)
Fraud:    492 cases — 0.172% of all transactions
Source:   ULB Machine Learning Group via Kaggle
```

> The raw CSV is excluded from this repository (284 MB). Download it from [Kaggle](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) and place it at `data/creditcard.csv`.

---

## CI / CD

GitHub Actions runs on every pull request to `main`:

- `dbt compile` — validates all SQL models parse correctly against the schema
- Fails fast if any model has a syntax error or broken reference

See [`.github/workflows/dbt_ci.yml`](.github/workflows/dbt_ci.yml).

---

## Roadmap

| # | Project | Stack | Status |
|---|---|---|---|
| 1 | ELT Pipeline | PostgreSQL · dbt · Airflow · Great Expectations · Docker | ✅ Complete |
| 2 | Streaming Ingestion | Kafka · Flink · PostgreSQL | Planned |
| 3 | ML Feature Store | Feast · MLflow · Python | Planned |
| 4 | Cloud Migration | Terraform · Snowflake · AWS S3 | Planned |
