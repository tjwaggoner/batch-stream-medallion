CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_alerts;

APPLY CHANGES INTO ${bronze_schema}.bronze_alerts
FROM STREAM(prebronze_alerts)
KEYS (alert_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
