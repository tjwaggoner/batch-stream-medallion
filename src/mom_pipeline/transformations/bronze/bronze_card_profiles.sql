CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_card_profiles;

APPLY CHANGES INTO ${bronze_schema}.bronze_card_profiles
FROM STREAM(prebronze_card_profiles)
KEYS (card_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
