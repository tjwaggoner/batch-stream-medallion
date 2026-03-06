CREATE OR REFRESH STREAMING TABLE prebronze_ach_payments
CLUSTER BY (payment_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/ach_payments/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'payment_id STRING, account_id STRING, amount DECIMAL(10,2), payment_date DATE, direction STRING, status STRING, ach_code STRING'
);
