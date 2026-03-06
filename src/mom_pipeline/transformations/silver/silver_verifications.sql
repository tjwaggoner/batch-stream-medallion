CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.silver_verifications
AS
SELECT
  vc.check_id,
  vc.user_id,
  vc.check_type,
  vc.result,
  vc.score,
  vc.check_date,
  vc.provider,
  vl.login_ts AS last_login_ts,
  vl.status AS login_status,
  vl.session_duration_ms
FROM ${bronze_schema}.bronze_verification_checks vc
LEFT JOIN (
  SELECT user_id, login_ts, status, session_duration_ms,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_ts DESC) AS rn
  FROM ${bronze_schema}.bronze_verification_logins
) vl ON vc.user_id = vl.user_id AND vl.rn = 1;
