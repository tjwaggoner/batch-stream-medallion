CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_linked_accounts;

APPLY CHANGES INTO ${bronze_schema}.bronze_linked_accounts
FROM STREAM(prebronze_linked_accounts)
KEYS (linked_acct_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
