CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_wire_transfers;

APPLY CHANGES INTO ${bronze_schema}.bronze_wire_transfers
FROM STREAM(prebronze_wire_transfers)
KEYS (transfer_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
