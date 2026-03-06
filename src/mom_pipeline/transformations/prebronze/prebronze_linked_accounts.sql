CREATE OR REFRESH STREAMING TABLE prebronze_linked_accounts
CLUSTER BY (linked_acct_id)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/linked_accounts/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'linked_acct_id STRING, user_id STRING, institution_name STRING, account_type STRING, routing_number STRING, last_four STRING, linked_date DATE, status STRING'
);
