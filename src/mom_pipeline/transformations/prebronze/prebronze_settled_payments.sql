CREATE OR REFRESH STREAMING TABLE prebronze_settled_payments
CLUSTER BY (settle_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/settled_payments/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'payment_id STRING, account_id STRING, amount DECIMAL(10,2), currency STRING, settle_date DATE, original_payment_date DATE, payment_method STRING'
);
