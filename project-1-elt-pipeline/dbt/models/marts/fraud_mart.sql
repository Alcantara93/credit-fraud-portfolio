/*
  fraud_mart  (Gold layer)
  ------------------------
  Analytics-ready fact table for the fraud investigation team.
  Includes engineered features and a rule-based risk score.
*/

{{ config(materialized='table', schema='gold') }}

SELECT
    transaction_time_seconds,
    transaction_hour,
    transaction_amount,
    amount_bucket,
    velocity_1h,
    is_fraud,

    -- Key PCA features known to be highly predictive
    pca_feature_01,
    pca_feature_02,
    pca_feature_03,
    pca_feature_04,
    pca_feature_05,
    pca_feature_14,
    pca_feature_17,

    -- Rule-based risk score (explainable for compliance / FINMA)
    CASE
        WHEN velocity_1h > 10 AND transaction_amount > 500 THEN 'HIGH'
        WHEN velocity_1h > 5  OR  transaction_amount > 200 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_score,

    ingested_at

FROM {{ ref('int_fraud_features') }}
