CREATE OR REFRESH STREAMING TABLE prebronze_card_profiles
CLUSTER BY (card_id)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/card_profiles/',
  format => 'json',
  schemaEvolutionMode => 'rescue',
  schemaHints => 'card_id STRING, account_id STRING, user_id STRING, card_type STRING, network STRING, last_four STRING, expiry_date STRING, status STRING, issued_date DATE'
);
