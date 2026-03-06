CREATE OR REFRESH STREAMING TABLE prebronze_portal_searches
CLUSTER BY (search_ts)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/portal_searches/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'user_id STRING, search_term STRING, search_ts TIMESTAMP, results_count INT, search_type STRING'
);
