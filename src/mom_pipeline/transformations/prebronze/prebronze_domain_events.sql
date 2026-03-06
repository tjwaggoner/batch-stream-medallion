CREATE OR REFRESH STREAMING TABLE prebronze_domain_events
CLUSTER BY (event_ts)
AS
SELECT
  CAST(key AS STRING) AS event_key,
  CAST(value AS STRING) AS event_value,
  from_json(CAST(value AS STRING), 'event_id STRING, entity_type STRING, entity_id STRING, event_type STRING, event_ts TIMESTAMP, payload STRING') AS parsed,
  topic,
  partition,
  offset,
  timestamp AS kafka_timestamp,
  current_timestamp() AS _ingested_at,
  NULL AS _source_file,
  'stream' AS _source_type
FROM read_stream(
  format => 'kafka',
  `kafka.bootstrap.servers` => '${kafka_brokers}',
  subscribe => 'finserv.domain_events',
  startingOffsets => 'latest'
);
