CREATE OR REFRESH STREAMING TABLE prebronze_case_records
CLUSTER BY (created_at)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/case_records/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'case_id STRING, case_type STRING, account_id STRING, status STRING, outcome STRING, assigned_to STRING, created_at TIMESTAMP, closed_at TIMESTAMP'
);
