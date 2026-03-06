CREATE OR REFRESH STREAMING TABLE prebronze_wire_transfers
CLUSTER BY (transfer_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  'batch' AS _source_type
FROM STREAM read_files(
  '${source_volume}/wire_transfers/',
  format => 'csv',
  header => true,
  schemaEvolutionMode => 'rescue',
  schemaHints => 'transfer_id STRING, card_id STRING, recipient STRING, amount DECIMAL(10,2), currency STRING, transfer_date TIMESTAMP, status STRING, network STRING'
);
