CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_domain_events
CLUSTER BY (event_ts)
AS
SELECT
  event_id,
  entity_type,
  entity_id,
  event_type,
  event_ts,
  payload,
  _ingested_at
FROM STREAM(prebronze_domain_events);
