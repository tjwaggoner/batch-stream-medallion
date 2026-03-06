CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.gold_fact_user_activity
AS
SELECT
  u.user_id,
  CAST(t.payment_date AS DATE) AS activity_date,
  COUNT(DISTINCT t.payment_id) AS txn_count,
  COALESCE(SUM(t.amount), 0) AS total_spend,
  COUNT(DISTINCT a.alert_id) AS alert_count
FROM ${silver_schema}.silver_users u
LEFT JOIN ${silver_schema}.silver_transactions t ON u.user_id = t.account_id
LEFT JOIN ${silver_schema}.silver_alerts a
  ON u.user_id = a.user_id
  AND CAST(a.created_at AS DATE) = CAST(t.payment_date AS DATE)
WHERE u.__END_AT IS NULL
GROUP BY u.user_id, CAST(t.payment_date AS DATE);
