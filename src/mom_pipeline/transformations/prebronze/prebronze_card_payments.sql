CREATE OR REFRESH STREAMING TABLE prebronze_card_payments
CLUSTER BY (payment_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/card_payments/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'payment_id STRING, card_id STRING, merchant STRING, amount DECIMAL(10,2), currency STRING, payment_date TIMESTAMP, status STRING, mcc_code STRING'
);
