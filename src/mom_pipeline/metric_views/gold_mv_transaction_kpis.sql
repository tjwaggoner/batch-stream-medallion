CREATE OR REPLACE VIEW ${gold_schema}.gold_mv_transaction_kpis
WITH METRICS
LANGUAGE YAML
$$
version: 1.1
source:
  table: gold_fact_daily_transactions
  catalog: waggoner_mom
  schema: gold
dimensions:
  - name: payment_date
    column: payment_date
  - name: payment_type
    column: payment_type
  - name: account_id
    column: account_id
measures:
  - name: total_transactions
    column: txn_count
    agg: sum
  - name: total_volume
    column: total_amount
    agg: sum
  - name: avg_transaction_value
    column: total_amount
    agg: avg
materialization:
  schedule:
    cron: "* * * * *"
  mode: relaxed
  materialized_views:
    - name: txn_kpis_by_type_date
      type: aggregated
      dimensions:
        - payment_date
        - payment_type
      measures:
        - total_transactions
        - total_volume
        - avg_transaction_value
$$;
