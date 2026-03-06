CREATE OR REFRESH STREAMING TABLE prebronze_verification_logins
CLUSTER BY (login_ts)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/verification_logins/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'user_id STRING, login_ts TIMESTAMP, status STRING, provider STRING, session_duration_ms INT'
);
