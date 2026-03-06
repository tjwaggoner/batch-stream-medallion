CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_dispute_status_changes;

APPLY CHANGES INTO ${bronze_schema}.bronze_dispute_status_changes
FROM STREAM(prebronze_dispute_status_changes)
KEYS (dispute_id, change_ts)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
