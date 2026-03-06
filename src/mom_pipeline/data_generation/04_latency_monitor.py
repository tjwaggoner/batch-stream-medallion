# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Latency Monitor
# MAGIC Measures end-to-end latency from data generation through each layer to gold.
# MAGIC All calculations done in SQL to avoid Python timezone issues.
# MAGIC Logs results to `waggoner_mom.prebronze._latency_metrics`.

# COMMAND ----------

spark.sql("USE CATALOG waggoner_mom")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measure and Calculate Latency (all in SQL)

# COMMAND ----------

latency_df = spark.sql("""
WITH timestamps AS (
  SELECT
    current_timestamp() AS run_ts,
    (SELECT MAX(_ingested_at) FROM prebronze.prebronze_alerts) AS prebronze_latest,
    (SELECT MAX(_ingested_at) FROM bronze.bronze_alerts) AS bronze_latest,
    current_timestamp() AS gold_last_refresh,
    (SELECT MAX(event_ts) FROM prebronze.prebronze_domain_events) AS domain_latest,
    (SELECT MAX(domain_event_ts) FROM silver.silver_transactions WHERE domain_event_ts IS NOT NULL) AS domain_in_silver
)
SELECT
  run_ts,
  prebronze_latest,
  bronze_latest,
  gold_last_refresh,
  domain_latest,
  domain_in_silver,
  ROUND(unix_timestamp(bronze_latest) - unix_timestamp(prebronze_latest), 1) AS prebronze_to_bronze_sec,
  ROUND(unix_timestamp(gold_last_refresh) - unix_timestamp(prebronze_latest), 1) AS prebronze_to_gold_sec,
  ROUND(ABS(unix_timestamp(domain_in_silver) - unix_timestamp(domain_latest)), 1) AS stream_to_silver_sec,
  CASE WHEN (unix_timestamp(gold_last_refresh) - unix_timestamp(prebronze_latest)) < 900 THEN true ELSE false END AS batch_sla_pass,
  CASE WHEN ABS(unix_timestamp(domain_in_silver) - unix_timestamp(domain_latest)) < 300 THEN true ELSE false END AS stream_sla_pass
FROM timestamps
""")

row = latency_df.collect()[0]

print(f"Run timestamp:           {row['run_ts']}")
print(f"Latest prebronze alert:  {row['prebronze_latest']}")
print(f"Latest bronze alert:     {row['bronze_latest']}")
print(f"Gold last refresh:       {row['gold_last_refresh']}")
print(f"Latest domain event:     {row['domain_latest']}")
print(f"Domain event in silver:  {row['domain_in_silver']}")
print()
print(f"--- Latency Results ---")
print(f"Prebronze -> Bronze:     {row['prebronze_to_bronze_sec']}s")
print(f"Prebronze -> Gold:       {row['prebronze_to_gold_sec']}s")
print(f"Stream event -> Silver:  {row['stream_to_silver_sec']}s")
print()
print(f"Batch SLA (<15 min):     {'PASS' if row['batch_sla_pass'] else 'FAIL'} ({row['prebronze_to_gold_sec']}s vs 900s)")
print(f"Stream SLA (<5 min):     {'PASS' if row['stream_sla_pass'] else 'FAIL'} ({row['stream_to_silver_sec']}s vs 300s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Metrics to Delta Table

# COMMAND ----------

latency_df.write.mode("append").saveAsTable("waggoner_mom.prebronze._latency_metrics")
print("Metrics logged to waggoner_mom.prebronze._latency_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latency Dashboard

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

metrics_hist = spark.sql("""
  SELECT run_ts, prebronze_to_bronze_sec, prebronze_to_gold_sec, stream_to_silver_sec,
         batch_sla_pass, stream_sla_pass
  FROM waggoner_mom.prebronze._latency_metrics
  ORDER BY run_ts DESC
  LIMIT 20
""").toPandas()

if len(metrics_hist) > 0:
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle("Pipeline Latency Monitor", fontsize=14, fontweight="bold")

    # Bar chart: latest run latencies
    labels = ["Prebronze→Bronze", "Prebronze→Gold", "Stream→Silver"]
    values = [
        row["prebronze_to_bronze_sec"] or 0,
        row["prebronze_to_gold_sec"] or 0,
        row["stream_to_silver_sec"] or 0
    ]
    sla_limits = [None, 900, 300]
    colors = ["#1f77b4", "#2ca02c" if row["batch_sla_pass"] else "#d62728",
              "#2ca02c" if row["stream_sla_pass"] else "#d62728"]

    bars = axes[0].bar(labels, values, color=colors)
    if sla_limits[1]:
        axes[0].axhline(y=900, color="red", linestyle="--", alpha=0.5, label="Batch SLA (900s)")
    if sla_limits[2]:
        axes[0].axhline(y=300, color="orange", linestyle="--", alpha=0.5, label="Stream SLA (300s)")
    axes[0].set_ylabel("Seconds")
    axes[0].set_title("Current Run Latency")
    axes[0].legend(fontsize=8)
    for bar, val in zip(bars, values):
        axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                     f"{val:.0f}s", ha="center", va="bottom", fontsize=9)

    # Trend: batch latency over time
    if len(metrics_hist) > 1:
        hist_sorted = metrics_hist.sort_values("run_ts")
        axes[1].plot(range(len(hist_sorted)), hist_sorted["prebronze_to_gold_sec"], "o-", color="#2ca02c", label="Batch")
        axes[1].axhline(y=900, color="red", linestyle="--", alpha=0.5, label="SLA (900s)")
        axes[1].set_xlabel("Run #")
        axes[1].set_ylabel("Seconds")
        axes[1].set_title("Batch Latency Trend")
        axes[1].legend(fontsize=8)
    else:
        axes[1].text(0.5, 0.5, "Need more runs\nfor trend", ha="center", va="center", transform=axes[1].transAxes)
        axes[1].set_title("Batch Latency Trend")

    # SLA scoreboard
    axes[2].axis("off")
    batch_color = "#2ca02c" if row["batch_sla_pass"] else "#d62728"
    stream_color = "#2ca02c" if row["stream_sla_pass"] else "#d62728"
    batch_text = "PASS" if row["batch_sla_pass"] else "FAIL"
    stream_text = "PASS" if row["stream_sla_pass"] else "FAIL"

    axes[2].text(0.5, 0.75, "Batch SLA (<15 min)", ha="center", va="center", fontsize=14, transform=axes[2].transAxes)
    axes[2].text(0.5, 0.6, batch_text, ha="center", va="center", fontsize=28, fontweight="bold",
                 color=batch_color, transform=axes[2].transAxes)
    axes[2].text(0.5, 0.35, "Stream SLA (<5 min)", ha="center", va="center", fontsize=14, transform=axes[2].transAxes)
    axes[2].text(0.5, 0.2, stream_text, ha="center", va="center", fontsize=28, fontweight="bold",
                 color=stream_color, transform=axes[2].transAxes)
    axes[2].set_title("SLA Status")

    plt.tight_layout()
    plt.show()
else:
    print("No metrics history yet — run the job at least once.")
