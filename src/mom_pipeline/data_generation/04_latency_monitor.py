# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Latency & Throughput Monitor
# MAGIC Measures per-layer timing and throughput by comparing row counts and timestamps
# MAGIC before and after the pipeline refresh.

# COMMAND ----------

spark.sql("USE CATALOG waggoner_mom")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-Layer Row Counts & Timestamps

# COMMAND ----------

layer_stats_df = spark.sql("""
WITH prebronze_stats AS (
  SELECT 'prebronze' AS layer,
    COUNT(*) AS total_rows,
    MIN(_ingested_at) AS earliest_ts,
    MAX(_ingested_at) AS latest_ts
  FROM prebronze.prebronze_alerts
),
bronze_stats AS (
  SELECT 'bronze' AS layer,
    COUNT(*) AS total_rows,
    MIN(_ingested_at) AS earliest_ts,
    MAX(_ingested_at) AS latest_ts
  FROM bronze.bronze_alerts
),
silver_stats AS (
  SELECT 'silver' AS layer,
    (SELECT COUNT(*) FROM silver.silver_transactions) +
    (SELECT COUNT(*) FROM silver.silver_users) +
    (SELECT COUNT(*) FROM silver.silver_accounts) +
    (SELECT COUNT(*) FROM silver.silver_alerts) AS total_rows,
    NULL AS earliest_ts,
    NULL AS latest_ts
),
gold_stats AS (
  SELECT 'gold' AS layer,
    (SELECT COUNT(*) FROM gold.gold_fact_daily_transactions) +
    (SELECT COUNT(*) FROM gold.gold_fact_user_activity) +
    (SELECT COUNT(*) FROM gold.gold_fact_risk_operations) +
    (SELECT COUNT(*) FROM gold.gold_dim_users) +
    (SELECT COUNT(*) FROM gold.gold_dim_accounts) +
    (SELECT COUNT(*) FROM gold.gold_dim_cards) AS total_rows,
    NULL AS earliest_ts,
    NULL AS latest_ts
)
SELECT * FROM prebronze_stats
UNION ALL SELECT * FROM bronze_stats
UNION ALL SELECT * FROM silver_stats
UNION ALL SELECT * FROM gold_stats
""")

layer_rows = layer_stats_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Timing (from _ingested_at timestamps)

# COMMAND ----------

timing_df = spark.sql("""
SELECT
  current_timestamp() AS monitor_ts,

  -- Prebronze: when Auto Loader picked up the latest file
  (SELECT MAX(_ingested_at) FROM prebronze.prebronze_alerts) AS prebronze_ingested_at,

  -- Bronze: when the latest CDC merge wrote
  (SELECT MAX(_ingested_at) FROM bronze.bronze_alerts) AS bronze_ingested_at,

  -- Pipeline processing time: bronze ingestion - prebronze ingestion
  ROUND(
    unix_timestamp((SELECT MAX(_ingested_at) FROM bronze.bronze_alerts)) -
    unix_timestamp((SELECT MAX(_ingested_at) FROM prebronze.prebronze_alerts)),
  1) AS prebronze_to_bronze_sec,

  -- Total pipeline time: now (post-refresh) - prebronze ingestion
  ROUND(
    unix_timestamp(current_timestamp()) -
    unix_timestamp((SELECT MAX(_ingested_at) FROM prebronze.prebronze_alerts)),
  1) AS ingestion_to_now_sec
""")

timing = timing_df.collect()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("  PIPELINE LATENCY & THROUGHPUT")
print("=" * 70)
print()
print(f"  {'Layer':<12} {'Total Rows':>14} {'Latest Timestamp':>28}")
print(f"  {'─'*12} {'─'*14} {'─'*28}")
for r in layer_rows:
    ts_str = str(r['latest_ts'])[:19] if r['latest_ts'] else '(materialized view)'
    print(f"  {r['layer']:<12} {r['total_rows']:>14,} {ts_str:>28}")

print()
print(f"  ─── Timing ───")
print(f"  Prebronze ingested at:     {timing['prebronze_ingested_at']}")
print(f"  Bronze ingested at:        {timing['bronze_ingested_at']}")
print(f"  Monitor ran at:            {timing['monitor_ts']}")
print()
print(f"  Prebronze → Bronze:        {timing['prebronze_to_bronze_sec']}s")
print(f"  Ingestion → Gold (approx): {timing['ingestion_to_now_sec']}s")
print()

total_latency = timing['ingestion_to_now_sec'] or 0
sla_pass = total_latency < 900
print(f"  Batch SLA (<15 min):       {'PASS' if sla_pass else 'FAIL'} ({total_latency:.0f}s / 900s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Metrics

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS waggoner_mom.prebronze._latency_metrics")

log_df = spark.sql(f"""
SELECT
  current_timestamp() AS run_ts,
  'prebronze' AS layer, {layer_rows[0]['total_rows']} AS total_rows,
  {timing['prebronze_to_bronze_sec'] or 0} AS layer_latency_sec,
  {total_latency} AS cumulative_latency_sec
UNION ALL SELECT current_timestamp(), 'bronze', {layer_rows[1]['total_rows']}, 0, 0
UNION ALL SELECT current_timestamp(), 'silver', {layer_rows[2]['total_rows']}, 0, 0
UNION ALL SELECT current_timestamp(), 'gold', {layer_rows[3]['total_rows']}, 0, {total_latency}
""")
log_df.write.mode("overwrite").saveAsTable("waggoner_mom.prebronze._latency_metrics")
print("Logged to waggoner_mom.prebronze._latency_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latency & Throughput Charts

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("Medallion Pipeline: Latency & Throughput", fontsize=14, fontweight="bold")

layers = [r['layer'] for r in layer_rows]
row_counts = [r['total_rows'] or 0 for r in layer_rows]
bar_colors = {"prebronze": "#3b82f6", "bronze": "#8b5cf6", "silver": "#f59e0b", "gold": "#10b981"}
colors = [bar_colors.get(l, "#999") for l in layers]

# --- Left: Row counts per layer ---
bars = ax1.bar(layers, row_counts, color=colors, width=0.5, edgecolor="white", linewidth=1)
for i, (b, cnt) in enumerate(zip(bars, row_counts)):
    ax1.text(i, cnt + max(row_counts) * 0.02, f"{cnt:,}", ha="center", va="bottom", fontsize=10, fontweight="bold")
ax1.set_ylabel("Total Rows")
ax1.set_title("Rows per Layer")
ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"{x:,.0f}"))

# --- Right: Timing breakdown ---
timing_labels = ["Prebronze→Bronze", "Ingestion→Gold"]
timing_values = [timing['prebronze_to_bronze_sec'] or 0, total_latency]
timing_colors = ["#8b5cf6", "#10b981" if sla_pass else "#ef4444"]

bars2 = ax2.barh(timing_labels, timing_values, color=timing_colors, height=0.4)
ax2.axvline(x=900, color="red", linestyle="--", alpha=0.5, linewidth=1.5, label="SLA (900s)")
for i, (b, val) in enumerate(zip(bars2, timing_values)):
    ax2.text(val + 5, i, f"{val:.0f}s", ha="left", va="center", fontsize=11, fontweight="bold")
ax2.set_xlabel("Seconds")
ax2.set_title(f"Pipeline Latency  |  {'PASS' if sla_pass else 'FAIL'}",
              color="#10b981" if sla_pass else "#ef4444", fontsize=11)
ax2.legend(fontsize=9)

plt.tight_layout()
plt.show()
