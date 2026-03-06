CREATE OR REFRESH STREAMING TABLE prebronze_portal_logins
CLUSTER BY (login_ts)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/portal_logins/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'user_id STRING, login_ts TIMESTAMP, ip_address STRING, user_agent STRING, success BOOLEAN'
);
