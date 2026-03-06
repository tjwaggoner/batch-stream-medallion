CREATE OR REFRESH STREAMING TABLE prebronze_account_balances
CLUSTER BY (account_id)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/account_balances/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'account_id STRING, user_id STRING, balance DECIMAL(12,2), available_balance DECIMAL(12,2), as_of_date DATE, currency STRING, account_type STRING'
);
