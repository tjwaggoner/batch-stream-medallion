CREATE OR REFRESH STREAMING TABLE ${silver_schema}.silver_cards;

APPLY CHANGES INTO ${silver_schema}.silver_cards
FROM STREAM(${bronze_schema}.bronze_card_profiles)
KEYS (card_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_ingested_at)
STORED AS SCD TYPE 2;
