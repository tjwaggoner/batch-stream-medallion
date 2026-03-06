CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.gold_dim_users
AS
SELECT
  user_id,
  full_name,
  email,
  phone,
  signup_date,
  status,
  city,
  state,
  zip
FROM ${silver_schema}.silver_users
WHERE __END_AT IS NULL;
