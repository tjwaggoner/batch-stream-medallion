CREATE OR REFRESH STREAMING TABLE prebronze_portal_users
CLUSTER BY (user_id)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/portal_users/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'user_id STRING, username STRING, role STRING, department STRING, status STRING, created_at TIMESTAMP, last_active TIMESTAMP'
);
