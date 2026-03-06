CREATE OR REFRESH STREAMING TABLE prebronze_payment_events
CLUSTER BY (event_ts)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/payment_events/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'event_id STRING, card_id STRING, event_type STRING, amount DECIMAL(10,2), merchant STRING, event_ts TIMESTAMP, status STRING'
);
