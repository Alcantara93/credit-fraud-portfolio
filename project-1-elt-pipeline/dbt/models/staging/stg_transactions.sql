/*
  stg_transactions
  ----------------
  Bronze → Silver: standardise column names and cast types.
  Materialized as a VIEW so it always reflects the latest raw data.
*/

{{ config(materialized='view', schema='silver') }}

SELECT
    -- Temporal
    "Time"                  AS transaction_time_seconds,

    -- Financial
    "Amount"                AS transaction_amount,

    -- Target label
    "Class"                 AS is_fraud,

    -- PCA-anonymised risk features (renamed for clarity)
    "V1"                    AS pca_feature_01,
    "V2"                    AS pca_feature_02,
    "V3"                    AS pca_feature_03,
    "V4"                    AS pca_feature_04,
    "V5"                    AS pca_feature_05,
    "V6"                    AS pca_feature_06,
    "V7"                    AS pca_feature_07,
    "V8"                    AS pca_feature_08,
    "V9"                    AS pca_feature_09,
    "V10"                   AS pca_feature_10,
    "V11"                   AS pca_feature_11,
    "V12"                   AS pca_feature_12,
    "V13"                   AS pca_feature_13,
    "V14"                   AS pca_feature_14,   -- high-importance fraud signal
    "V15"                   AS pca_feature_15,
    "V16"                   AS pca_feature_16,
    "V17"                   AS pca_feature_17,   -- high-importance fraud signal
    "V18"                   AS pca_feature_18,
    "V19"                   AS pca_feature_19,
    "V20"                   AS pca_feature_20,
    "V21"                   AS pca_feature_21,
    "V22"                   AS pca_feature_22,
    "V23"                   AS pca_feature_23,
    "V24"                   AS pca_feature_24,
    "V25"                   AS pca_feature_25,
    "V26"                   AS pca_feature_26,
    "V27"                   AS pca_feature_27,
    "V28"                   AS pca_feature_28,

    -- Metadata
    ingested_at

FROM {{ source('raw', 'transactions') }}
