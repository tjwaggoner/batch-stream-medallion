CREATE OR REFRESH STREAMING TABLE prebronze_portal_activity
CLUSTER BY (timestamp)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/portal_activity/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'session_id STRING, user_id STRING, action STRING, page STRING, ip_address STRING, timestamp TIMESTAMP, duration_ms INT'
);
