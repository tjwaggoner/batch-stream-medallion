CREATE OR REFRESH STREAMING TABLE prebronze_verification_checks
CLUSTER BY (check_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/verification_checks/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'check_id STRING, user_id STRING, check_type STRING, result STRING, score INT, check_date TIMESTAMP, provider STRING'
);
