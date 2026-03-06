CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.silver_accounts
AS
SELECT
  b.account_id,
  b.user_id,
  b.balance,
  b.available_balance,
  b.as_of_date,
  b.currency,
  b.account_type,
  la.linked_acct_id,
  la.institution_name,
  la.routing_number,
  la.last_four AS linked_last_four,
  la.linked_date,
  la.status AS linked_status
FROM ${bronze_schema}.bronze_account_balances b
LEFT JOIN ${bronze_schema}.bronze_linked_accounts la
  ON b.user_id = la.user_id;
