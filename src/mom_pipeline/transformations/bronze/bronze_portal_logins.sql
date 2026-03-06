CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_portal_logins;

APPLY CHANGES INTO ${bronze_schema}.bronze_portal_logins
FROM STREAM(prebronze_portal_logins)
KEYS (user_id, login_ts)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
