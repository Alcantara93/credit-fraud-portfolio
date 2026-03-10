/*
  reporting_mart  (Gold layer)
  ----------------------------
  Hourly aggregates for business dashboards and executive reporting.
  Answers: How many transactions and what fraud rate occurred each hour?
*/

{{ config(materialized='table', schema='gold') }}

SELECT
    transaction_hour,

    COUNT(*)                                                                        AS total_transactions,
    SUM(is_fraud)                                                                   AS total_fraud_transactions,
    ROUND(AVG(is_fraud::FLOAT) * 100, 4)                                            AS fraud_rate_pct,

    ROUND(AVG(transaction_amount)::NUMERIC, 2)                                      AS avg_transaction_amount,
    ROUND(SUM(transaction_amount)::NUMERIC, 2)                                      AS total_transaction_volume,
    ROUND(
        SUM(CASE WHEN is_fraud = 1 THEN transaction_amount ELSE 0 END)::NUMERIC, 2
    )                                                                               AS total_fraud_volume,

    COUNT(CASE WHEN risk_score = 'HIGH'   THEN 1 END)                               AS high_risk_count,
    COUNT(CASE WHEN risk_score = 'MEDIUM' THEN 1 END)                               AS medium_risk_count,
    COUNT(CASE WHEN risk_score = 'LOW'    THEN 1 END)                               AS low_risk_count

FROM {{ ref('fraud_mart') }}
GROUP BY transaction_hour
ORDER BY transaction_hour
