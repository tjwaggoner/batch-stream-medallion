CREATE OR REPLACE VIEW ${gold_schema}.gold_mv_risk_operations
WITH METRICS
LANGUAGE YAML
$$
version: 1.1
source:
  table: gold_fact_risk_operations
  catalog: waggoner_mom
  schema: gold
dimensions:
  - name: operation_month
    expr: "DATE_TRUNC('MONTH', operation_date)"
  - name: operation_type
    column: operation_type
measures:
  - name: total_operations
    column: operation_count
    agg: sum
  - name: total_disputes
    column: dispute_count
    agg: sum
  - name: total_disputed_amount
    column: total_disputed_amount
    agg: sum
  - name: avg_disputed_amount
    column: avg_disputed_amount
    agg: avg
materialization:
  schedule:
    cron: "* * * * *"
  mode: relaxed
  materialized_views:
    - name: risk_ops_monthly
      type: aggregated
      dimensions:
        - operation_month
        - operation_type
      measures:
        - total_operations
        - total_disputes
        - total_disputed_amount
$$;
