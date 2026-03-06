CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.gold_dim_accounts
AS
SELECT
  account_id,
  user_id,
  balance,
  available_balance,
  as_of_date,
  currency,
  account_type,
  institution_name,
  linked_status
FROM ${silver_schema}.silver_accounts;
