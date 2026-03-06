CREATE OR REFRESH STREAMING TABLE prebronze_dispute_records
CLUSTER BY (created_at)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/dispute_records/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'dispute_id STRING, account_id STRING, transaction_id STRING, amount DECIMAL(10,2), reason STRING, status STRING, created_at TIMESTAMP'
);
