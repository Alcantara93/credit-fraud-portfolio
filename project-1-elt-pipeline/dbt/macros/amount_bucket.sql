/*
  amount_bucket(amount_col)
  -------------------------
  Categorises a transaction amount into a spend tier.
  Reusable across all models — avoids duplicating CASE logic.

  Tiers:
    micro   < €10
    small   €10 – €99
    medium  €100 – €999
    large   ≥ €1,000
*/

{% macro amount_bucket(amount_col) %}
    CASE
        WHEN {{ amount_col }} <    10  THEN 'micro'
        WHEN {{ amount_col }} <   100  THEN 'small'
        WHEN {{ amount_col }} <  1000  THEN 'medium'
        ELSE                                'large'
    END
{% endmacro %}
