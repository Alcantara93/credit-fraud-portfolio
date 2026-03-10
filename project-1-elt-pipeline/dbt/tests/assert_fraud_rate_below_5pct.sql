/*
  assert_fraud_rate_below_5pct
  ----------------------------
  Singular test: fails (returns rows) if the overall fraud rate
  in int_fraud_features exceeds 5%.

  The Kaggle dataset has a ~0.172% fraud rate. Any value ≥ 5%
  would indicate a data quality issue (e.g. wrong file loaded).
*/

SELECT 1
FROM (
    SELECT ROUND(AVG(is_fraud::FLOAT) * 100, 4) AS fraud_rate
    FROM {{ ref('int_fraud_features') }}
) subquery
WHERE fraud_rate >= 5
