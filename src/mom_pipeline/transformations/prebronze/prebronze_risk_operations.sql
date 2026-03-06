CREATE OR REFRESH STREAMING TABLE prebronze_risk_operations
CLUSTER BY (op_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/risk_operations/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'op_id STRING, card_id STRING, op_type STRING, op_date TIMESTAMP, amount DECIMAL(10,2), risk_score INT, decision STRING'
);
