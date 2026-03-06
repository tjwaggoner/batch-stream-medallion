CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.silver_cards_enriched
AS
SELECT
  cp.card_id,
  cp.account_id,
  cp.user_id,
  cp.card_type,
  cp.network,
  cp.last_four,
  cp.expiry_date,
  cp.status,
  cp.issued_date,
  COUNT(pe.event_id) AS total_events,
  MAX(pe.event_ts) AS last_event_ts,
  SUM(pe.amount) AS total_event_amount
FROM ${bronze_schema}.bronze_card_profiles cp
LEFT JOIN ${bronze_schema}.bronze_payment_events pe ON cp.card_id = pe.card_id
GROUP BY cp.card_id, cp.account_id, cp.user_id, cp.card_type, cp.network,
         cp.last_four, cp.expiry_date, cp.status, cp.issued_date;
