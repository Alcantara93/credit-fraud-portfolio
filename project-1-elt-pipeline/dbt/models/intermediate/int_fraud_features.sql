/*
  int_fraud_features
  ------------------
  Derives engineered risk signals on top of stg_transactions:
    • transaction_hour     — hour-of-day bucket from elapsed seconds
    • amount_bucket        — categorical spend tier (macro)
    • velocity_1h          — rolling transaction count within the same hour window
*/

{{ config(materialized='table', schema='silver') }}

SELECT
    -- Pass-through from staging
    transaction_time_seconds,
    transaction_amount,
    is_fraud,
    pca_feature_01,
    pca_feature_02,
    pca_feature_03,
    pca_feature_04,
    pca_feature_05,
    pca_feature_06,
    pca_feature_07,
    pca_feature_08,
    pca_feature_09,
    pca_feature_10,
    pca_feature_11,
    pca_feature_12,
    pca_feature_13,
    pca_feature_14,
    pca_feature_15,
    pca_feature_16,
    pca_feature_17,
    pca_feature_18,
    pca_feature_19,
    pca_feature_20,
    pca_feature_21,
    pca_feature_22,
    pca_feature_23,
    pca_feature_24,
    pca_feature_25,
    pca_feature_26,
    pca_feature_27,
    pca_feature_28,
    ingested_at,

    -- Engineered features
    FLOOR(transaction_time_seconds / 3600)::INTEGER         AS transaction_hour,

    {{ amount_bucket('transaction_amount') }}                AS amount_bucket,

    COUNT(*) OVER (
        PARTITION BY FLOOR(transaction_time_seconds / 3600)
        ORDER BY transaction_time_seconds
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                        AS velocity_1h

FROM {{ ref('stg_transactions') }}
