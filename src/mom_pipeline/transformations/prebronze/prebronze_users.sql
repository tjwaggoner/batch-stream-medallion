CREATE OR REFRESH STREAMING TABLE prebronze_users
CLUSTER BY (user_id)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/users/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'user_id STRING, full_name STRING, email STRING, phone STRING, signup_date DATE, status STRING, address STRING, city STRING, state STRING, zip STRING'
);
