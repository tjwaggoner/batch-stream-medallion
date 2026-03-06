CREATE OR REFRESH STREAMING TABLE prebronze_rule_summaries
CLUSTER BY (summary_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/rule_summaries/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'rule_id STRING, rule_name STRING, summary_date DATE, hit_count INT, block_count INT, review_count INT'
);
