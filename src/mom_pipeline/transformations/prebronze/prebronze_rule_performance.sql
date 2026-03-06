CREATE OR REFRESH STREAMING TABLE prebronze_rule_performance
CLUSTER BY (evaluation_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/rule_performance/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'rule_id STRING, rule_name STRING, accounts_affected INT, false_positive_rate DECIMAL(5,4), true_positive_rate DECIMAL(5,4), evaluation_date DATE'
);
