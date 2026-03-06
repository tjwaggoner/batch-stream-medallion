CREATE OR REPLACE VIEW ${gold_schema}.gold_mv_user_health
WITH METRICS
LANGUAGE YAML
$$
version: 1.1
source:
  table: gold_fact_user_activity
  catalog: waggoner_mom
  schema: gold
dimensions:
  - name: activity_month
    expr: "DATE_TRUNC('MONTH', activity_date)"
  - name: user_id
    column: user_id
measures:
  - name: active_users
    column: user_id
    agg: count_distinct
  - name: avg_spend_per_user
    column: total_spend
    agg: avg
  - name: total_alerts
    column: alert_count
    agg: sum
materialization:
  schedule:
    cron: "0 */2 * * *"
  mode: relaxed
  materialized_views:
    - name: user_health_monthly
      type: aggregated
      dimensions:
        - activity_month
      measures:
        - active_users
        - avg_spend_per_user
        - total_alerts
$$;
