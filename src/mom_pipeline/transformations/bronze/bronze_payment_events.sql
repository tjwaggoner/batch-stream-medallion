CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_payment_events;

APPLY CHANGES INTO ${bronze_schema}.bronze_payment_events
FROM STREAM(prebronze_payment_events)
KEYS (event_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
