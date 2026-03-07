# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Latency Monitor
# MAGIC Measures hop-by-hop latency across each medallion layer.
# MAGIC Logs results to `waggoner_mom.prebronze._latency_metrics`.

# COMMAND ----------

spark.sql("USE CATALOG waggoner_mom")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measure Timestamps at Each Layer

# COMMAND ----------

latency_df = spark.sql("""
WITH layer_timestamps AS (
  SELECT
    current_timestamp() AS run_ts,

    -- Pre-bronze: when the latest file was ingested
    (SELECT MAX(_ingested_at) FROM prebronze.prebronze_alerts) AS prebronze_ts,

    -- Bronze: when the latest CDC merge completed
    (SELECT MAX(_ingested_at) FROM bronze.bronze_alerts) AS bronze_ts,

    -- Silver: latest transaction record (materialized view refresh)
    (SELECT MAX(payment_date) FROM silver.silver_transactions) AS silver_batch_ts,

    -- Silver: latest domain event that made it through
    (SELECT MAX(domain_event_ts) FROM silver.silver_transactions WHERE domain_event_ts IS NOT NULL) AS silver_stream_ts,

    -- Gold: proxy via current_timestamp (runs right after pipeline refresh)
    current_timestamp() AS gold_ts,

    -- Streaming: latest event generated
    (SELECT MAX(event_ts) FROM prebronze.prebronze_domain_events) AS stream_origin_ts
)
SELECT
  run_ts,
  prebronze_ts,
  bronze_ts,
  silver_batch_ts,
  silver_stream_ts,
  gold_ts,
  stream_origin_ts,

  -- Hop latencies (batch path)
  ROUND(unix_timestamp(bronze_ts) - unix_timestamp(prebronze_ts), 1) AS hop_prebronze_to_bronze,
  ROUND(unix_timestamp(gold_ts) - unix_timestamp(bronze_ts), 1) AS hop_bronze_to_gold,
  ROUND(unix_timestamp(gold_ts) - unix_timestamp(prebronze_ts), 1) AS total_batch_latency,

  -- Hop latencies (stream path)
  ROUND(unix_timestamp(prebronze_ts) - unix_timestamp(stream_origin_ts), 1) AS hop_source_to_prebronze,
  ROUND(ABS(unix_timestamp(silver_stream_ts) - unix_timestamp(stream_origin_ts)), 1) AS total_stream_latency,

  -- SLA checks
  CASE WHEN (unix_timestamp(gold_ts) - unix_timestamp(prebronze_ts)) < 900 THEN true ELSE false END AS batch_sla_pass,
  CASE WHEN ABS(unix_timestamp(silver_stream_ts) - unix_timestamp(stream_origin_ts)) < 300 THEN true ELSE false END AS stream_sla_pass
FROM layer_timestamps
""")

row = latency_df.collect()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latency Summary

# COMMAND ----------

print("=" * 60)
print("  BATCH PATH (file → prebronze → bronze → silver → gold)")
print("=" * 60)
print(f"  Pre-bronze timestamp:  {row['prebronze_ts']}")
print(f"  Bronze timestamp:      {row['bronze_ts']}")
print(f"  Gold timestamp:        {row['gold_ts']}")
print()
print(f"  Prebronze → Bronze:    {row['hop_prebronze_to_bronze']}s")
print(f"  Bronze → Gold:         {row['hop_bronze_to_gold']}s")
print(f"  ─────────────────────────────────")
print(f"  TOTAL (end-to-end):    {row['total_batch_latency']}s")
print(f"  SLA Target:            900s (15 min)")
print(f"  Status:                {'PASS' if row['batch_sla_pass'] else 'FAIL'}")
print()
print("=" * 60)
print("  STREAM PATH (event → prebronze → bronze → silver)")
print("=" * 60)
print(f"  Event origin:          {row['stream_origin_ts']}")
print(f"  Silver arrival:        {row['silver_stream_ts']}")
print()
print(f"  Source → Pre-bronze:   {row['hop_source_to_prebronze']}s")
print(f"  ─────────────────────────────────")
print(f"  TOTAL (end-to-end):    {row['total_stream_latency']}s")
print(f"  SLA Target:            300s (5 min)")
print(f"  Status:                {'PASS' if row['stream_sla_pass'] else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Metrics

# COMMAND ----------

# Drop and recreate to handle schema changes (metrics table, old data is stale)
spark.sql("DROP TABLE IF EXISTS waggoner_mom.prebronze._latency_metrics")
latency_df.write.mode("overwrite").saveAsTable("waggoner_mom.prebronze._latency_metrics")
print("Logged to waggoner_mom.prebronze._latency_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Latency Waterfall

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), gridspec_kw={'height_ratios': [3, 2]})
fig.suptitle("Medallion Pipeline Latency", fontsize=15, fontweight="bold", y=0.98)

# --- Top: Waterfall chart showing hop-by-hop latency ---
layers = ["Pre-bronze", "Bronze", "Silver", "Gold"]
batch_hops = [
    0,
    row['hop_prebronze_to_bronze'] or 0,
    0,  # bronze→silver is implicit in the MV refresh
    row['hop_bronze_to_gold'] or 0
]
# Cumulative for waterfall
cumulative = [0]
for h in batch_hops[1:]:
    cumulative.append(cumulative[-1] + h)

bar_colors = ["#3b82f6", "#8b5cf6", "#f59e0b", "#10b981"]
bar_width = 0.5

# Draw waterfall bars
for i, (layer, cum, hop) in enumerate(zip(layers, cumulative, batch_hops)):
    if i == 0:
        ax1.bar(i, 0.1, bottom=0, width=bar_width, color=bar_colors[i], label=layer)
        ax1.text(i, 0.5, "Origin", ha="center", va="bottom", fontsize=9, fontweight="bold")
    else:
        ax1.bar(i, hop, bottom=cumulative[i] - hop, width=bar_width, color=bar_colors[i], label=layer)
        ax1.text(i, cumulative[i] + 2, f"+{hop:.0f}s", ha="center", va="bottom", fontsize=10, fontweight="bold")
    # Connector line
    if i < len(layers) - 1:
        ax1.plot([i + bar_width/2, i + 1 - bar_width/2], [cumulative[i], cumulative[i]],
                 color="gray", linestyle=":", linewidth=1)

# SLA line
total_batch = row['total_batch_latency'] or 0
ax1.axhline(y=900, color="red", linestyle="--", alpha=0.6, linewidth=1.5)
ax1.text(len(layers) - 0.5, 905, "Batch SLA (15 min)", color="red", fontsize=9, ha="right")

ax1.set_xticks(range(len(layers)))
ax1.set_xticklabels(layers, fontsize=11)
ax1.set_ylabel("Cumulative Latency (seconds)", fontsize=10)
ax1.set_title(f"Batch Path: End-to-End = {total_batch:.0f}s  |  {'PASS' if row['batch_sla_pass'] else 'FAIL'}",
              fontsize=12, color="#10b981" if row["batch_sla_pass"] else "#ef4444")

# --- Bottom: Stream path + SLA scoreboard ---
ax2.axis("off")

# Stream path arrow diagram
stream_total = row['total_stream_latency'] or 0
stream_pass = row['stream_sla_pass']

# Draw flow diagram
positions = [0.1, 0.3, 0.5, 0.7]
stream_labels = ["Kafka Event", "Pre-bronze", "Bronze", "Silver"]
stream_colors = ["#3b82f6", "#3b82f6", "#8b5cf6", "#f59e0b"]

for i, (pos, label, color) in enumerate(zip(positions, stream_labels, stream_colors)):
    ax2.add_patch(plt.Rectangle((pos - 0.06, 0.55), 0.12, 0.3, facecolor=color, alpha=0.15,
                                edgecolor=color, linewidth=2, transform=ax2.transAxes))
    ax2.text(pos, 0.7, label, ha="center", va="center", fontsize=10, fontweight="bold",
             transform=ax2.transAxes)
    if i < len(positions) - 1:
        ax2.annotate("", xy=(positions[i+1] - 0.06, 0.7), xytext=(pos + 0.06, 0.7),
                     xycoords="axes fraction", textcoords="axes fraction",
                     arrowprops=dict(arrowstyle="->", color="gray", lw=1.5))

# SLA boxes
ax2.add_patch(plt.Rectangle((0.78, 0.55), 0.2, 0.3, facecolor="#10b981" if stream_pass else "#ef4444",
                             alpha=0.15, edgecolor="#10b981" if stream_pass else "#ef4444",
                             linewidth=2, transform=ax2.transAxes))
ax2.text(0.88, 0.75, "Stream SLA", ha="center", va="center", fontsize=9, transform=ax2.transAxes)
ax2.text(0.88, 0.63, f"{'PASS' if stream_pass else 'FAIL'} ({stream_total:.0f}s / 300s)",
         ha="center", va="center", fontsize=11, fontweight="bold",
         color="#10b981" if stream_pass else "#ef4444", transform=ax2.transAxes)

# Batch SLA summary below
ax2.text(0.1, 0.2, f"Batch:  {total_batch:.0f}s / 900s", fontsize=12, transform=ax2.transAxes,
         fontfamily="monospace")
ax2.text(0.45, 0.2, "PASS" if row["batch_sla_pass"] else "FAIL", fontsize=12, fontweight="bold",
         color="#10b981" if row["batch_sla_pass"] else "#ef4444", transform=ax2.transAxes)
ax2.text(0.1, 0.05, f"Stream: {stream_total:.0f}s / 300s", fontsize=12, transform=ax2.transAxes,
         fontfamily="monospace")
ax2.text(0.45, 0.05, "PASS" if stream_pass else "FAIL", fontsize=12, fontweight="bold",
         color="#10b981" if stream_pass else "#ef4444", transform=ax2.transAxes)

plt.tight_layout()
plt.show()
