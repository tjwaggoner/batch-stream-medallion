CREATE OR REFRESH STREAMING TABLE prebronze_dispute_status_changes
CLUSTER BY (change_ts)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/dispute_status/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'dispute_id STRING, old_status STRING, new_status STRING, change_ts TIMESTAMP, changed_by STRING, reason STRING'
);
