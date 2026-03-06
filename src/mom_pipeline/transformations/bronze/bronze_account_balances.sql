CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_account_balances;

APPLY CHANGES INTO ${bronze_schema}.bronze_account_balances
FROM STREAM(prebronze_account_balances)
KEYS (account_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
