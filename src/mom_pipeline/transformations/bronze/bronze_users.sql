CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_users;

APPLY CHANGES INTO ${bronze_schema}.bronze_users
FROM STREAM(prebronze_users)
KEYS (user_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
