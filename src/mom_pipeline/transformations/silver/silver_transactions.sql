CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.silver_transactions
AS
WITH unified_payments AS (
  SELECT
    payment_id,
    account_id,
    NULL AS card_id,
    amount,
    payment_date,
    direction,
    status,
    'ach' AS payment_type
  FROM ${bronze_schema}.bronze_ach_payments

  UNION ALL

  SELECT
    transfer_id AS payment_id,
    NULL AS account_id,
    card_id,
    amount,
    CAST(transfer_date AS DATE) AS payment_date,
    'outbound' AS direction,
    status,
    'wire' AS payment_type
  FROM ${bronze_schema}.bronze_wire_transfers

  UNION ALL

  SELECT
    payment_id,
    NULL AS account_id,
    card_id,
    amount,
    CAST(payment_date AS DATE) AS payment_date,
    'outbound' AS direction,
    status,
    'card' AS payment_type
  FROM ${bronze_schema}.bronze_card_payments

  UNION ALL

  SELECT
    payment_id,
    account_id,
    NULL AS card_id,
    amount,
    settle_date AS payment_date,
    'inbound' AS direction,
    'settled' AS status,
    'settled' AS payment_type
  FROM ${bronze_schema}.bronze_settled_payments
),
domain_events AS (
  SELECT
    event_id,
    entity_id,
    entity_type,
    event_type,
    event_ts,
    payload
  FROM ${bronze_schema}.bronze_domain_events
  WHERE entity_type = 'payment'
)
SELECT
  COALESCE(p.payment_id, d.entity_id) AS payment_id,
  p.account_id,
  p.card_id,
  p.amount,
  p.payment_date,
  p.direction,
  p.status,
  p.payment_type,
  c.card_type,
  c.network AS card_network,
  d.event_type AS domain_event_type,
  d.event_ts AS domain_event_ts
FROM unified_payments p
LEFT JOIN domain_events d ON p.payment_id = d.entity_id
LEFT JOIN ${bronze_schema}.bronze_card_profiles c ON p.card_id = c.card_id;
