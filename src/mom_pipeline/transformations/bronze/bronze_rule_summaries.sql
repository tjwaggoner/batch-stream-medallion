CREATE OR REFRESH STREAMING TABLE ${bronze_schema}.bronze_rule_summaries;

APPLY CHANGES INTO ${bronze_schema}.bronze_rule_summaries
FROM STREAM(prebronze_rule_summaries)
KEYS (rule_id, summary_date)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _source_type, _rescued_data)
STORED AS SCD TYPE 1;
