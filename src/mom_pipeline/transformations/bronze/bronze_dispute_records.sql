CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_dispute_records;

APPLY CHANGES INTO ${bronze_schema}.bronze_dispute_records
FROM STREAM(prebronze_dispute_records)
KEYS (dispute_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
