-- Domain events: file-based simulation (reads JSON files from volume)
-- In production, replace with read_kafka() for real Kafka ingestion
CREATE OR REFRESH STREAMING TABLE prebronze_domain_events
CLUSTER BY (event_ts)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'stream' AS _source_type
FROM STREAM read_files(
  '${source_volume}/domain_events/',
  format => 'json',
  schemaEvolutionMode => 'rescue',
  schemaHints => 'event_id STRING, entity_type STRING, entity_id STRING, event_type STRING, event_ts TIMESTAMP, payload STRING'
);
