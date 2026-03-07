# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Latency & Throughput Monitor
# MAGIC Measures per-layer row counts, data size (batch vs stream), and latency.

# COMMAND ----------

spark.sql("USE CATALOG waggoner_mom")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-Layer Row Counts, Size, and Batch vs Stream Breakdown

# COMMAND ----------

# Get detailed stats per table including size
table_stats_df = spark.sql("""
WITH table_info AS (
  -- Prebronze tables
  SELECT 'prebronze' AS layer, 'prebronze_users' AS table_name, 'batch' AS source_type, COUNT(*) AS row_count FROM prebronze.prebronze_users
  UNION ALL SELECT 'prebronze', 'prebronze_linked_accounts', 'batch', COUNT(*) FROM prebronze.prebronze_linked_accounts
  UNION ALL SELECT 'prebronze', 'prebronze_account_balances', 'batch', COUNT(*) FROM prebronze.prebronze_account_balances
  UNION ALL SELECT 'prebronze', 'prebronze_alerts', 'batch', COUNT(*) FROM prebronze.prebronze_alerts
  UNION ALL SELECT 'prebronze', 'prebronze_payment_events', 'batch', COUNT(*) FROM prebronze.prebronze_payment_events
  UNION ALL SELECT 'prebronze', 'prebronze_card_payments', 'batch', COUNT(*) FROM prebronze.prebronze_card_payments
  UNION ALL SELECT 'prebronze', 'prebronze_settled_payments', 'batch', COUNT(*) FROM prebronze.prebronze_settled_payments
  UNION ALL SELECT 'prebronze', 'prebronze_wire_transfers', 'batch', COUNT(*) FROM prebronze.prebronze_wire_transfers
  UNION ALL SELECT 'prebronze', 'prebronze_ach_payments', 'batch', COUNT(*) FROM prebronze.prebronze_ach_payments
  UNION ALL SELECT 'prebronze', 'prebronze_card_profiles', 'batch', COUNT(*) FROM prebronze.prebronze_card_profiles
  UNION ALL SELECT 'prebronze', 'prebronze_verification_checks', 'batch', COUNT(*) FROM prebronze.prebronze_verification_checks
  UNION ALL SELECT 'prebronze', 'prebronze_portal_activity', 'batch', COUNT(*) FROM prebronze.prebronze_portal_activity
  UNION ALL SELECT 'prebronze', 'prebronze_risk_operations', 'batch', COUNT(*) FROM prebronze.prebronze_risk_operations
  UNION ALL SELECT 'prebronze', 'prebronze_dispute_records', 'batch', COUNT(*) FROM prebronze.prebronze_dispute_records
  UNION ALL SELECT 'prebronze', 'prebronze_dispute_status_changes', 'batch', COUNT(*) FROM prebronze.prebronze_dispute_status_changes
  UNION ALL SELECT 'prebronze', 'prebronze_case_records', 'batch', COUNT(*) FROM prebronze.prebronze_case_records
  UNION ALL SELECT 'prebronze', 'prebronze_rule_performance', 'batch', COUNT(*) FROM prebronze.prebronze_rule_performance
  UNION ALL SELECT 'prebronze', 'prebronze_rule_summaries', 'batch', COUNT(*) FROM prebronze.prebronze_rule_summaries
  UNION ALL SELECT 'prebronze', 'prebronze_portal_logins', 'batch', COUNT(*) FROM prebronze.prebronze_portal_logins
  UNION ALL SELECT 'prebronze', 'prebronze_portal_searches', 'batch', COUNT(*) FROM prebronze.prebronze_portal_searches
  UNION ALL SELECT 'prebronze', 'prebronze_portal_users', 'batch', COUNT(*) FROM prebronze.prebronze_portal_users
  UNION ALL SELECT 'prebronze', 'prebronze_verification_logins', 'batch', COUNT(*) FROM prebronze.prebronze_verification_logins
  UNION ALL SELECT 'prebronze', 'prebronze_domain_events', 'stream', COUNT(*) FROM prebronze.prebronze_domain_events
  -- Bronze tables
  UNION ALL SELECT 'bronze', 'bronze_users', 'batch', COUNT(*) FROM bronze.bronze_users
  UNION ALL SELECT 'bronze', 'bronze_linked_accounts', 'batch', COUNT(*) FROM bronze.bronze_linked_accounts
  UNION ALL SELECT 'bronze', 'bronze_account_balances', 'batch', COUNT(*) FROM bronze.bronze_account_balances
  UNION ALL SELECT 'bronze', 'bronze_alerts', 'batch', COUNT(*) FROM bronze.bronze_alerts
  UNION ALL SELECT 'bronze', 'bronze_payment_events', 'batch', COUNT(*) FROM bronze.bronze_payment_events
  UNION ALL SELECT 'bronze', 'bronze_card_payments', 'batch', COUNT(*) FROM bronze.bronze_card_payments
  UNION ALL SELECT 'bronze', 'bronze_settled_payments', 'batch', COUNT(*) FROM bronze.bronze_settled_payments
  UNION ALL SELECT 'bronze', 'bronze_wire_transfers', 'batch', COUNT(*) FROM bronze.bronze_wire_transfers
  UNION ALL SELECT 'bronze', 'bronze_ach_payments', 'batch', COUNT(*) FROM bronze.bronze_ach_payments
  UNION ALL SELECT 'bronze', 'bronze_card_profiles', 'batch', COUNT(*) FROM bronze.bronze_card_profiles
  UNION ALL SELECT 'bronze', 'bronze_verification_checks', 'batch', COUNT(*) FROM bronze.bronze_verification_checks
  UNION ALL SELECT 'bronze', 'bronze_verification_logins', 'batch', COUNT(*) FROM bronze.bronze_verification_logins
  UNION ALL SELECT 'bronze', 'bronze_portal_activity', 'batch', COUNT(*) FROM bronze.bronze_portal_activity
  UNION ALL SELECT 'bronze', 'bronze_portal_logins', 'batch', COUNT(*) FROM bronze.bronze_portal_logins
  UNION ALL SELECT 'bronze', 'bronze_portal_searches', 'batch', COUNT(*) FROM bronze.bronze_portal_searches
  UNION ALL SELECT 'bronze', 'bronze_portal_users', 'batch', COUNT(*) FROM bronze.bronze_portal_users
  UNION ALL SELECT 'bronze', 'bronze_risk_operations', 'batch', COUNT(*) FROM bronze.bronze_risk_operations
  UNION ALL SELECT 'bronze', 'bronze_rule_performance', 'batch', COUNT(*) FROM bronze.bronze_rule_performance
  UNION ALL SELECT 'bronze', 'bronze_rule_summaries', 'batch', COUNT(*) FROM bronze.bronze_rule_summaries
  UNION ALL SELECT 'bronze', 'bronze_dispute_records', 'batch', COUNT(*) FROM bronze.bronze_dispute_records
  UNION ALL SELECT 'bronze', 'bronze_dispute_status_changes', 'batch', COUNT(*) FROM bronze.bronze_dispute_status_changes
  UNION ALL SELECT 'bronze', 'bronze_case_records', 'batch', COUNT(*) FROM bronze.bronze_case_records
  UNION ALL SELECT 'bronze', 'bronze_domain_events', 'stream', COUNT(*) FROM bronze.bronze_domain_events
  -- Silver
  UNION ALL SELECT 'silver', 'silver_users', 'batch', COUNT(*) FROM silver.silver_users
  UNION ALL SELECT 'silver', 'silver_cards', 'batch', COUNT(*) FROM silver.silver_cards
  UNION ALL SELECT 'silver', 'silver_accounts', 'batch', COUNT(*) FROM silver.silver_accounts
  UNION ALL SELECT 'silver', 'silver_transactions', 'mixed', COUNT(*) FROM silver.silver_transactions
  UNION ALL SELECT 'silver', 'silver_cards_enriched', 'batch', COUNT(*) FROM silver.silver_cards_enriched
  UNION ALL SELECT 'silver', 'silver_alerts', 'batch', COUNT(*) FROM silver.silver_alerts
  UNION ALL SELECT 'silver', 'silver_verifications', 'batch', COUNT(*) FROM silver.silver_verifications
  UNION ALL SELECT 'silver', 'silver_portal_activity', 'batch', COUNT(*) FROM silver.silver_portal_activity
  UNION ALL SELECT 'silver', 'silver_risk_operations', 'batch', COUNT(*) FROM silver.silver_risk_operations
  -- Gold
  UNION ALL SELECT 'gold', 'gold_fact_daily_transactions', 'mixed', COUNT(*) FROM gold.gold_fact_daily_transactions
  UNION ALL SELECT 'gold', 'gold_fact_user_activity', 'mixed', COUNT(*) FROM gold.gold_fact_user_activity
  UNION ALL SELECT 'gold', 'gold_fact_risk_operations', 'batch', COUNT(*) FROM gold.gold_fact_risk_operations
  UNION ALL SELECT 'gold', 'gold_agg_risk_summary', 'mixed', COUNT(*) FROM gold.gold_agg_risk_summary
  UNION ALL SELECT 'gold', 'gold_dim_users', 'batch', COUNT(*) FROM gold.gold_dim_users
  UNION ALL SELECT 'gold', 'gold_dim_accounts', 'batch', COUNT(*) FROM gold.gold_dim_accounts
  UNION ALL SELECT 'gold', 'gold_dim_cards', 'batch', COUNT(*) FROM gold.gold_dim_cards
)
SELECT * FROM table_info
""")

table_stats_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Sizes (from information_schema)

# COMMAND ----------

# Get table sizes in bytes from information_schema
size_df = spark.sql("""
SELECT
  table_schema AS schema_name,
  table_name,
  CAST(data_length AS BIGINT) AS size_bytes
FROM waggoner_mom.information_schema.tables
WHERE table_schema IN ('prebronze', 'bronze', 'silver', 'gold')
  AND table_name NOT LIKE '__%'
  AND table_name NOT LIKE 'event_log%'
""")

size_map = {}
for r in size_df.collect():
    key = f"{r['schema_name']}.{r['table_name']}"
    size_map[key] = r['size_bytes'] or 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

table_data = table_stats_df.collect()

# Aggregate by layer
from collections import defaultdict
layer_agg = defaultdict(lambda: {"batch_rows": 0, "stream_rows": 0, "mixed_rows": 0, "total_rows": 0, "size_bytes": 0, "tables": 0})

for r in table_data:
    layer = r['layer']
    rows = r['row_count']
    src = r['source_type']
    tbl_key = f"{layer}.{r['table_name']}"
    tbl_size = size_map.get(tbl_key, 0)

    layer_agg[layer]["total_rows"] += rows
    layer_agg[layer]["size_bytes"] += tbl_size
    layer_agg[layer]["tables"] += 1
    if src == 'batch':
        layer_agg[layer]["batch_rows"] += rows
    elif src == 'stream':
        layer_agg[layer]["stream_rows"] += rows
    else:
        layer_agg[layer]["mixed_rows"] += rows

def fmt_size(b):
    if b >= 1e9: return f"{b/1e9:.1f} GB"
    if b >= 1e6: return f"{b/1e6:.1f} MB"
    if b >= 1e3: return f"{b/1e3:.1f} KB"
    return f"{b} B"

layer_order = ['prebronze', 'bronze', 'silver', 'gold']

print("=" * 90)
print("  PIPELINE THROUGHPUT")
print("=" * 90)
print()
print(f"  {'Layer':<12} {'Tables':>7} {'Total Rows':>14} {'Batch Rows':>14} {'Stream Rows':>14} {'Size':>10}")
print(f"  {'─'*12} {'─'*7} {'─'*14} {'─'*14} {'─'*14} {'─'*10}")
for layer in layer_order:
    a = layer_agg[layer]
    stream_display = a['stream_rows'] + a['mixed_rows']
    print(f"  {layer:<12} {a['tables']:>7} {a['total_rows']:>14,} {a['batch_rows']:>14,} {stream_display:>14,} {fmt_size(a['size_bytes']):>10}")

total_rows = sum(layer_agg[l]['total_rows'] for l in layer_order)
total_size = sum(layer_agg[l]['size_bytes'] for l in layer_order)
print(f"  {'─'*12} {'─'*7} {'─'*14} {'─'*14} {'─'*14} {'─'*10}")
print(f"  {'TOTAL':<12} {'':<7} {total_rows:>14,} {'':<14} {'':<14} {fmt_size(total_size):>10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Timing

# COMMAND ----------

timing_df = spark.sql("""
SELECT
  current_timestamp() AS monitor_ts,
  (SELECT MAX(_ingested_at) FROM prebronze.prebronze_alerts) AS prebronze_ingested_at,
  ROUND(
    unix_timestamp(current_timestamp()) -
    unix_timestamp((SELECT MAX(_ingested_at) FROM prebronze.prebronze_alerts)),
  1) AS ingestion_to_gold_sec
""")

timing = timing_df.collect()[0]
total_latency = timing['ingestion_to_gold_sec'] or 0
sla_pass = total_latency < 900

print()
print(f"  ─── Timing ───")
print(f"  Prebronze ingested at:     {timing['prebronze_ingested_at']}")
print(f"  Monitor ran at:            {timing['monitor_ts']}")
print(f"  Ingestion → Gold (approx): {total_latency:.0f}s")
print()
print(f"  Batch SLA (<15 min):       {'PASS' if sla_pass else 'FAIL'} ({total_latency:.0f}s / 900s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Metrics

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS waggoner_mom.prebronze._latency_metrics")

log_rows = []
for layer in layer_order:
    a = layer_agg[layer]
    log_rows.append((layer, a['total_rows'], a['batch_rows'], a['stream_rows'] + a['mixed_rows'], a['size_bytes'], a['tables']))

from pyspark.sql.types import *
log_schema = StructType([
    StructField("layer", StringType()),
    StructField("total_rows", LongType()),
    StructField("batch_rows", LongType()),
    StructField("stream_rows", LongType()),
    StructField("size_bytes", LongType()),
    StructField("table_count", IntegerType())
])
log_df = spark.createDataFrame(log_rows, schema=log_schema)
log_df.write.mode("overwrite").saveAsTable("waggoner_mom.prebronze._latency_metrics")
print("Logged to waggoner_mom.prebronze._latency_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Charts

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

fig, axes = plt.subplots(1, 3, figsize=(18, 5))
fig.suptitle("Medallion Pipeline: Throughput & Latency", fontsize=14, fontweight="bold")

layers = layer_order
bar_colors = {"prebronze": "#3b82f6", "bronze": "#8b5cf6", "silver": "#f59e0b", "gold": "#10b981"}

# --- Chart 1: Rows per layer (stacked batch vs stream) ---
batch_rows = [layer_agg[l]['batch_rows'] for l in layers]
stream_rows = [layer_agg[l]['stream_rows'] + layer_agg[l]['mixed_rows'] for l in layers]

x = np.arange(len(layers))
w = 0.5
bars_batch = axes[0].bar(x, batch_rows, w, label='Batch', color='#3b82f6', alpha=0.8)
bars_stream = axes[0].bar(x, stream_rows, w, bottom=batch_rows, label='Stream', color='#f97316', alpha=0.8)

for i, (b, s) in enumerate(zip(batch_rows, stream_rows)):
    total = b + s
    axes[0].text(i, total + max(b + s for b, s in zip(batch_rows, stream_rows)) * 0.02,
                 f"{total:,}", ha="center", va="bottom", fontsize=9, fontweight="bold")

axes[0].set_xticks(x)
axes[0].set_xticklabels(layers, fontsize=10)
axes[0].set_ylabel("Rows")
axes[0].set_title("Rows per Layer (Batch vs Stream)")
axes[0].legend(fontsize=9)
axes[0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"{x:,.0f}"))

# --- Chart 2: Data size per layer ---
sizes_mb = [layer_agg[l]['size_bytes'] / 1e6 for l in layers]
colors = [bar_colors[l] for l in layers]

bars2 = axes[1].bar(layers, sizes_mb, color=colors, width=0.5, edgecolor="white", linewidth=1)
for i, (b, mb) in enumerate(zip(bars2, sizes_mb)):
    label = f"{mb:.1f} MB" if mb < 1000 else f"{mb/1000:.1f} GB"
    axes[1].text(i, mb + max(sizes_mb) * 0.02, label, ha="center", va="bottom", fontsize=10, fontweight="bold")

axes[1].set_ylabel("MB")
axes[1].set_title("Data Size per Layer")
axes[1].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"{x:,.0f}"))

# --- Chart 3: Latency with SLA ---
sla_color = "#10b981" if sla_pass else "#ef4444"
axes[2].barh(["Ingestion → Gold"], [total_latency], color=sla_color, height=0.3)
axes[2].axvline(x=900, color="red", linestyle="--", alpha=0.5, linewidth=1.5, label="SLA (900s)")
axes[2].text(total_latency + 5, 0, f"{total_latency:.0f}s", ha="left", va="center", fontsize=12, fontweight="bold")
axes[2].set_xlabel("Seconds")
axes[2].set_title(f"Pipeline Latency  |  {'PASS' if sla_pass else 'FAIL'}", color=sla_color, fontsize=11)
axes[2].legend(fontsize=9)
axes[2].set_xlim(0, max(total_latency * 1.3, 1000))

plt.tight_layout()
plt.show()
