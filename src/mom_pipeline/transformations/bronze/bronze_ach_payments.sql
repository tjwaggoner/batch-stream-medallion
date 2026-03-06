CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_ach_payments;

APPLY CHANGES INTO ${bronze_schema}.bronze_ach_payments
FROM STREAM(prebronze_ach_payments)
KEYS (payment_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
