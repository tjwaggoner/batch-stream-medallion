CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.silver_portal_activity
AS
WITH all_activity AS (
  SELECT
    session_id AS activity_id,
    user_id,
    action AS activity_type,
    page AS detail,
    timestamp AS activity_ts,
    duration_ms
  FROM ${bronze_schema}.bronze_portal_activity

  UNION ALL

  SELECT
    CONCAT('login_', user_id, '_', CAST(login_ts AS STRING)) AS activity_id,
    user_id,
    'login' AS activity_type,
    ip_address AS detail,
    login_ts AS activity_ts,
    NULL AS duration_ms
  FROM ${bronze_schema}.bronze_portal_logins

  UNION ALL

  SELECT
    CONCAT('search_', user_id, '_', CAST(search_ts AS STRING)) AS activity_id,
    user_id,
    'search' AS activity_type,
    search_term AS detail,
    search_ts AS activity_ts,
    NULL AS duration_ms
  FROM ${bronze_schema}.bronze_portal_searches
)
SELECT
  a.activity_id,
  a.user_id,
  a.activity_type,
  a.detail,
  a.activity_ts,
  a.duration_ms,
  pu.username,
  pu.role AS user_role,
  pu.department
FROM all_activity a
LEFT JOIN ${bronze_schema}.bronze_portal_users pu ON a.user_id = pu.user_id;
