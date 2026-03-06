CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.gold_agg_risk_summary
AS
SELECT
  t.payment_date,
  t.card_type,
  COUNT(CASE WHEN t.amount > 1000 THEN 1 END) AS high_value_txn_count,
  COUNT(DISTINCT t.account_id) AS distinct_accounts,
  COUNT(*) AS total_txn_count,
  SUM(t.amount) AS total_amount,
  SUM(CASE WHEN ro.operation_type = 'dispute' THEN COALESCE(ro.amount, 0) ELSE 0 END) AS total_flagged_amount
FROM ${silver_schema}.silver_transactions t
LEFT JOIN ${silver_schema}.silver_risk_operations ro
  ON t.payment_id = ro.operation_id
WHERE t.payment_date IS NOT NULL
GROUP BY t.payment_date, t.card_type;
