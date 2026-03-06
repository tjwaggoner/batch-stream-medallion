# Databricks notebook source
# MAGIC %md
# MAGIC # Create Materialized Metric Views

# COMMAND ----------

spark.sql("USE CATALOG waggoner_mom")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold_mv_transaction_kpis
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: waggoner_mom.gold.gold_fact_daily_transactions
dimensions:
  - name: payment_date
    expr: payment_date
  - name: payment_type
    expr: payment_type
  - name: account_id
    expr: account_id
measures:
  - name: total_transactions
    expr: SUM(txn_count)
  - name: total_volume
    expr: SUM(total_amount)
  - name: avg_transaction_value
    expr: SUM(total_amount) / NULLIF(SUM(txn_count), 0)
materialization:
  schedule: every 2 hours
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
$$
""")
print("Created gold_mv_transaction_kpis")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold_mv_user_health
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: waggoner_mom.gold.gold_fact_user_activity
dimensions:
  - name: activity_month
    expr: DATE_TRUNC('MONTH', activity_date)
  - name: user_id
    expr: user_id
measures:
  - name: active_users
    expr: COUNT(DISTINCT user_id)
  - name: avg_spend_per_user
    expr: AVG(total_spend)
  - name: total_alerts
    expr: SUM(alert_count)
materialization:
  schedule: every 2 hours
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
$$
""")
print("Created gold_mv_user_health")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold_mv_risk_operations
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: waggoner_mom.gold.gold_fact_risk_operations
dimensions:
  - name: operation_month
    expr: DATE_TRUNC('MONTH', operation_date)
  - name: operation_type
    expr: operation_type
measures:
  - name: total_operations
    expr: SUM(operation_count)
  - name: total_disputes
    expr: SUM(dispute_count)
  - name: total_disputed_amount
    expr: SUM(total_disputed_amount)
  - name: avg_disputed_amount
    expr: AVG(avg_disputed_amount)
materialization:
  schedule: every 2 hours
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
$$
""")
print("Created gold_mv_risk_operations")

# COMMAND ----------

result = spark.sql("SHOW VIEWS IN waggoner_mom.gold LIKE 'gold_mv_*'")
result.show()
print("All 3 materialized metric views created!")
