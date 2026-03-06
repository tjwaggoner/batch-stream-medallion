CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.gold_dim_cards
AS
SELECT
  card_id,
  account_id,
  user_id,
  card_type,
  network,
  last_four,
  expiry_date,
  status,
  issued_date
FROM ${silver_schema}.silver_cards
WHERE __END_AT IS NULL;
