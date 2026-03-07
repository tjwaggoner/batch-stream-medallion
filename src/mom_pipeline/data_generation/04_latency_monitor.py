# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Latency, Throughput & Cost Monitor
# MAGIC Measures per-layer row counts, data size (batch vs stream), latency, and estimated cost.

# COMMAND ----------

spark.sql("USE CATALOG waggoner_mom")

PIPELINE_ID = "2c9b65f9-3582-49f9-b1ee-43453f9b4dc9"

# Serverless pricing (AWS US-West-2 Oregon, Enterprise tier)
# Source: system.billing.list_prices
PRICING = {
    "SERVERLESS_PIPELINE": {"rate": 0.45, "sku": "ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON"},
    "SERVERLESS_NOTEBOOK": {"rate": 0.45, "sku": "ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON"},
    "SERVERLESS_SQL":      {"rate": 0.70, "sku": "ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_WEST_OREGON"},
}

# Estimated DBU consumption per minute of serverless compute
# Based on observed serverless pipeline runs (small-medium workloads)
DBUS_PER_MIN_PIPELINE = 0.75   # SDP pipeline refresh (serverless)
DBUS_PER_MIN_NOTEBOOK = 0.40   # Serverless notebooks (data gen)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-Layer Row Counts and Batch vs Stream Breakdown

# COMMAND ----------

table_stats_df = spark.sql("""
WITH table_info AS (
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
  UNION ALL SELECT 'silver', 'silver_users', 'batch', COUNT(*) FROM silver.silver_users
  UNION ALL SELECT 'silver', 'silver_cards', 'batch', COUNT(*) FROM silver.silver_cards
  UNION ALL SELECT 'silver', 'silver_accounts', 'batch', COUNT(*) FROM silver.silver_accounts
  UNION ALL SELECT 'silver', 'silver_transactions', 'mixed', COUNT(*) FROM silver.silver_transactions
  UNION ALL SELECT 'silver', 'silver_cards_enriched', 'batch', COUNT(*) FROM silver.silver_cards_enriched
  UNION ALL SELECT 'silver', 'silver_alerts', 'batch', COUNT(*) FROM silver.silver_alerts
  UNION ALL SELECT 'silver', 'silver_verifications', 'batch', COUNT(*) FROM silver.silver_verifications
  UNION ALL SELECT 'silver', 'silver_portal_activity', 'batch', COUNT(*) FROM silver.silver_portal_activity
  UNION ALL SELECT 'silver', 'silver_risk_operations', 'batch', COUNT(*) FROM silver.silver_risk_operations
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

table_data = table_stats_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Stats

# COMMAND ----------

from collections import defaultdict

layer_agg = defaultdict(lambda: {"batch_rows": 0, "stream_rows": 0, "mixed_rows": 0, "total_rows": 0, "size_bytes": 0, "tables": 0})
AVG_BYTES_PER_ROW = {"prebronze": 250, "bronze": 200, "silver": 300, "gold": 150}

for r in table_data:
    layer = r['layer']
    rows = r['row_count']
    src = r['source_type']
    layer_agg[layer]["total_rows"] += rows
    layer_agg[layer]["size_bytes"] += rows * AVG_BYTES_PER_ROW.get(layer, 200)
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
total_rows = sum(layer_agg[l]['total_rows'] for l in layer_order)
total_size = sum(layer_agg[l]['size_bytes'] for l in layer_order)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Timing

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Estimate

# COMMAND ----------

# Estimate based on observed pipeline run duration (~2 min) and notebook durations
# Pipeline refresh: ~2 min serverless compute
# Data gen notebooks: ~1.5 min each (batch gen) + ~1.5 min (stream gen) + ~1 min (this monitor)
pipeline_run_min = 2.0
notebook_run_min = 4.0  # total across all notebook tasks

pipeline_dbus = pipeline_run_min * DBUS_PER_MIN_PIPELINE
notebook_dbus = notebook_run_min * DBUS_PER_MIN_NOTEBOOK

pipeline_cost = pipeline_dbus * PRICING["SERVERLESS_PIPELINE"]["rate"]
notebook_cost = notebook_dbus * PRICING["SERVERLESS_NOTEBOOK"]["rate"]
total_cost = pipeline_cost + notebook_cost
total_dbus = pipeline_dbus + notebook_dbus

cost_per_million = (total_cost / total_rows * 1e6) if total_rows > 0 else 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 95)
print("  PIPELINE THROUGHPUT")
print("=" * 95)
print()
print(f"  {'Layer':<12} {'Tables':>7} {'Total Rows':>14} {'Batch':>14} {'Stream':>14} {'Est. Size':>10}")
print(f"  {'─'*12} {'─'*7} {'─'*14} {'─'*14} {'─'*14} {'─'*10}")
for layer in layer_order:
    a = layer_agg[layer]
    stream_display = a['stream_rows'] + a['mixed_rows']
    print(f"  {layer:<12} {a['tables']:>7} {a['total_rows']:>14,} {a['batch_rows']:>14,} {stream_display:>14,} {fmt_size(a['size_bytes']):>10}")
print(f"  {'─'*12} {'─'*7} {'─'*14} {'─'*14} {'─'*14} {'─'*10}")
print(f"  {'TOTAL':<12} {'':>7} {total_rows:>14,} {'':>14} {'':>14} {fmt_size(total_size):>10}")

print()
print(f"  ─── Timing ───")
print(f"  Ingestion → Gold:          {total_latency:.0f}s")
print(f"  Batch SLA (<15 min):       {'PASS' if sla_pass else 'FAIL'} ({total_latency:.0f}s / 900s)")

print()
print(f"  ─── Estimated Cost (per pipeline refresh) ───")
print(f"  Pipeline (SDP refresh):    {pipeline_dbus:.2f} DBUs × ${PRICING['SERVERLESS_PIPELINE']['rate']:.2f}/DBU = ${pipeline_cost:.4f}")
print(f"  Notebooks (data gen):      {notebook_dbus:.2f} DBUs × ${PRICING['SERVERLESS_NOTEBOOK']['rate']:.2f}/DBU = ${notebook_cost:.4f}")
print(f"  ────────────────────────────────────")
print(f"  Total DBUs:                {total_dbus:.2f}")
print(f"  Total cost:                ${total_cost:.4f}")
print(f"  Cost per 1M rows:          ${cost_per_million:.4f}")
print(f"  SKU:                       {PRICING['SERVERLESS_PIPELINE']['sku']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Charts

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle("Medallion Pipeline: Throughput, Latency & Cost", fontsize=15, fontweight="bold")

layers = layer_order
bar_colors = {"prebronze": "#3b82f6", "bronze": "#8b5cf6", "silver": "#f59e0b", "gold": "#10b981"}

# --- Chart 1 (top-left): Rows per layer (stacked batch vs stream) ---
batch_rows_list = [layer_agg[l]['batch_rows'] for l in layers]
stream_rows_list = [layer_agg[l]['stream_rows'] + layer_agg[l]['mixed_rows'] for l in layers]
x = np.arange(len(layers))
w = 0.5
axes[0,0].bar(x, batch_rows_list, w, label='Batch', color='#3b82f6', alpha=0.8)
axes[0,0].bar(x, stream_rows_list, w, bottom=batch_rows_list, label='Stream', color='#f97316', alpha=0.8)
for i, (b, s) in enumerate(zip(batch_rows_list, stream_rows_list)):
    axes[0,0].text(i, b + s + max(b + s for b, s in zip(batch_rows_list, stream_rows_list)) * 0.02,
                   f"{b+s:,}", ha="center", va="bottom", fontsize=9, fontweight="bold")
axes[0,0].set_xticks(x)
axes[0,0].set_xticklabels(layers, fontsize=10)
axes[0,0].set_ylabel("Rows")
axes[0,0].set_title("Rows per Layer (Batch vs Stream)")
axes[0,0].legend(fontsize=9)
axes[0,0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"{x:,.0f}"))

# --- Chart 2 (top-right): Data size per layer ---
sizes_mb = [layer_agg[l]['size_bytes'] / 1e6 for l in layers]
colors = [bar_colors[l] for l in layers]
bars2 = axes[0,1].bar(layers, sizes_mb, color=colors, width=0.5, edgecolor="white", linewidth=1)
for i, (b, mb) in enumerate(zip(bars2, sizes_mb)):
    label = f"{mb:.1f} MB" if mb < 1000 else f"{mb/1000:.1f} GB"
    axes[0,1].text(i, mb + max(sizes_mb) * 0.02, label, ha="center", va="bottom", fontsize=10, fontweight="bold")
axes[0,1].set_ylabel("MB (estimated)")
axes[0,1].set_title("Data Size per Layer")

# --- Chart 3 (bottom-left): Latency with SLA ---
sla_color = "#10b981" if sla_pass else "#ef4444"
axes[1,0].barh(["Ingestion → Gold"], [total_latency], color=sla_color, height=0.3)
axes[1,0].axvline(x=900, color="red", linestyle="--", alpha=0.5, linewidth=1.5, label="SLA (900s)")
axes[1,0].text(total_latency + 5, 0, f"{total_latency:.0f}s", ha="left", va="center", fontsize=12, fontweight="bold")
axes[1,0].set_xlabel("Seconds")
axes[1,0].set_title(f"Pipeline Latency  |  {'PASS' if sla_pass else 'FAIL'}", color=sla_color, fontsize=11)
axes[1,0].legend(fontsize=9)
axes[1,0].set_xlim(0, max(total_latency * 1.3, 1000))

# --- Chart 4 (bottom-right): Cost breakdown ---
cost_labels = ['Pipeline\n(SDP)', 'Notebooks\n(Data Gen)']
cost_values = [pipeline_cost, notebook_cost]
cost_colors = ['#6366f1', '#f97316']
bars4 = axes[1,1].bar(cost_labels, cost_values, color=cost_colors, width=0.4, edgecolor="white")
for i, (b, val) in enumerate(zip(bars4, cost_values)):
    axes[1,1].text(i, val + max(cost_values) * 0.05,
                   f"${val:.4f}\n({[pipeline_dbus, notebook_dbus][i]:.2f} DBUs)",
                   ha="center", va="bottom", fontsize=10, fontweight="bold")
axes[1,1].set_ylabel("USD")
axes[1,1].set_title(f"Est. Cost per Refresh: ${total_cost:.4f}")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Summary

# COMMAND ----------

print("─" * 70)
print(f"  Total rows across all layers:  {total_rows:,}")
print(f"  Estimated total data size:     {fmt_size(total_size)}")
print(f"  Ingestion → Gold latency:      {total_latency:.0f}s")
print(f"  Batch SLA (<15 min):           {'PASS' if sla_pass else 'FAIL'}")
print(f"  Estimated DBUs per refresh:    {total_dbus:.2f}")
print(f"  Estimated cost per refresh:    ${total_cost:.4f}")
print(f"  Cost per million rows:         ${cost_per_million:.4f}")
print(f"  Pricing rate:                  ${PRICING['SERVERLESS_PIPELINE']['rate']:.2f}/DBU")
print(f"  SKU:                           {PRICING['SERVERLESS_PIPELINE']['sku']}")
print(f"  Pipeline ID:                   {PIPELINE_ID}")
print("─" * 70)
