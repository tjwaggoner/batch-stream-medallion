CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_risk_operations;

APPLY CHANGES INTO ${bronze_schema}.bronze_risk_operations
FROM STREAM(prebronze_risk_operations)
KEYS (op_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
