CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_domain_events
CLUSTER BY (event_ts)
AS
SELECT
  parsed.event_id,
  parsed.entity_type,
  parsed.entity_id,
  parsed.event_type,
  parsed.event_ts,
  parsed.payload,
  kafka_timestamp,
  _ingested_at
FROM STREAM(prebronze_domain_events);
