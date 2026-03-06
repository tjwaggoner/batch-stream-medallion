CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_rule_performance;

APPLY CHANGES INTO ${bronze_schema}.bronze_rule_performance
FROM STREAM(prebronze_rule_performance)
KEYS (rule_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
