CREATE OR REFRESH STREAMING TABLE ${silver_schema}.silver_users;

APPLY CHANGES INTO ${silver_schema}.silver_users
FROM STREAM(${bronze_schema}.bronze_users)
KEYS (user_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_ingested_at)
STORED AS SCD TYPE 2;
