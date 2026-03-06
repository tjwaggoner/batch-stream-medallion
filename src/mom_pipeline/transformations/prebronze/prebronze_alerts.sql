CREATE OR REFRESH STREAMING TABLE prebronze_alerts
CLUSTER BY (created_at)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/alerts/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'alert_id STRING, user_id STRING, alert_type STRING, channel STRING, message STRING, created_at TIMESTAMP, read_at TIMESTAMP, status STRING'
);
