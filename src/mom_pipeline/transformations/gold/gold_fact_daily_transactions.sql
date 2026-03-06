CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.gold_fact_daily_transactions
AS
SELECT
  payment_date,
  payment_type,
  account_id,
  COUNT(*) AS txn_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount,
  MAX(amount) AS max_amount,
  MIN(amount) AS min_amount
FROM ${silver_schema}.silver_transactions
WHERE payment_date IS NOT NULL
GROUP BY payment_date, payment_type, account_id;
