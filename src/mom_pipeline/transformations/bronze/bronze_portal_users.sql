CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_portal_users;

APPLY CHANGES INTO ${bronze_schema}.bronze_portal_users
FROM STREAM(prebronze_portal_users)
KEYS (user_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
