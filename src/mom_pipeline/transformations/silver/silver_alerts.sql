CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.silver_alerts
AS
SELECT
  a.alert_id,
  a.user_id,
  a.alert_type,
  a.channel,
  a.message,
  a.created_at,
  a.read_at,
  a.status,
  pe.event_id AS related_event_id,
  pe.amount AS related_amount,
  pe.merchant AS related_merchant
FROM ${bronze_schema}.bronze_alerts a
LEFT JOIN ${bronze_schema}.bronze_payment_events pe
  ON a.user_id = pe.card_id
  AND pe.event_ts BETWEEN a.created_at - INTERVAL 1 HOUR AND a.created_at;
