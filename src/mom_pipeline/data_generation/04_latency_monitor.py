# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Latency & Throughput Monitor
# MAGIC Measures per-layer latency (ingestion → gold) and throughput using the SDP event log.
# MAGIC Logs results to `waggoner_mom.prebronze._latency_metrics`.

# COMMAND ----------

spark.sql("USE CATALOG waggoner_mom")

PIPELINE_ID = "2c9b65f9-3582-49f9-b1ee-43453f9b4dc9"
EVENT_LOG_TABLE = f"prebronze.event_log_{PIPELINE_ID.replace('-', '_')}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-Layer Completion Times (from SDP Event Log)

# COMMAND ----------

# Get the latest update's flow_progress events — these tell us when each dataset finished
layer_timing_df = spark.sql(f"""
WITH latest_update AS (
  SELECT origin_update_id
  FROM {EVENT_LOG_TABLE}
  WHERE event_type = 'update_progress'
  ORDER BY timestamp DESC
  LIMIT 1
),
flow_events AS (
  SELECT
    origin_dataset_name AS dataset,
    event_type,
    timestamp AS completed_at,
    details:flow_progress:metrics:num_output_rows AS rows_written,
    details:flow_progress:status AS flow_status
  FROM {EVENT_LOG_TABLE}
  WHERE origin_update_id = (SELECT origin_update_id FROM latest_update)
    AND event_type = 'flow_progress'
    AND details:flow_progress:status = 'COMPLETED'
)
SELECT
  dataset,
  completed_at,
  CAST(rows_written AS LONG) AS rows_written,
  CASE
    WHEN dataset LIKE 'prebronze%' THEN 'prebronze'
    WHEN dataset LIKE 'bronze%' THEN 'bronze'
    WHEN dataset LIKE 'silver%' THEN 'silver'
    WHEN dataset LIKE 'gold%' THEN 'gold'
    ELSE 'other'
  END AS layer
FROM flow_events
ORDER BY completed_at
""")

layer_timing_df.cache()
display(layer_timing_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latency: Per-Layer Start & End Times

# COMMAND ----------

layer_summary_df = spark.sql(f"""
WITH latest_update AS (
  SELECT origin_update_id
  FROM {EVENT_LOG_TABLE}
  WHERE event_type = 'update_progress'
  ORDER BY timestamp DESC
  LIMIT 1
),
flow_events AS (
  SELECT
    origin_dataset_name AS dataset,
    timestamp AS completed_at,
    CAST(details:flow_progress:metrics:num_output_rows AS LONG) AS rows_written,
    CASE
      WHEN origin_dataset_name LIKE 'prebronze%' THEN 'prebronze'
      WHEN origin_dataset_name LIKE 'bronze%' THEN 'bronze'
      WHEN origin_dataset_name LIKE 'silver%' THEN 'silver'
      WHEN origin_dataset_name LIKE 'gold%' THEN 'gold'
      ELSE 'other'
    END AS layer
  FROM {EVENT_LOG_TABLE}
  WHERE origin_update_id = (SELECT origin_update_id FROM latest_update)
    AND event_type = 'flow_progress'
    AND details:flow_progress:status = 'COMPLETED'
),
layer_agg AS (
  SELECT
    layer,
    MIN(completed_at) AS layer_start,
    MAX(completed_at) AS layer_end,
    SUM(rows_written) AS total_rows,
    COUNT(*) AS datasets_completed
  FROM flow_events
  WHERE layer != 'other'
  GROUP BY layer
),
pipeline_bounds AS (
  SELECT
    MIN(layer_start) AS pipeline_start,
    MAX(layer_end) AS pipeline_end
  FROM layer_agg
)
SELECT
  la.layer,
  la.layer_start,
  la.layer_end,
  ROUND(unix_timestamp(la.layer_end) - unix_timestamp(la.layer_start), 1) AS layer_duration_sec,
  ROUND(unix_timestamp(la.layer_end) - unix_timestamp(pb.pipeline_start), 1) AS cumulative_sec,
  la.total_rows,
  la.datasets_completed,
  ROUND(la.total_rows / NULLIF(unix_timestamp(la.layer_end) - unix_timestamp(la.layer_start), 0), 0) AS rows_per_sec
FROM layer_agg la
CROSS JOIN pipeline_bounds pb
ORDER BY
  CASE la.layer
    WHEN 'prebronze' THEN 1
    WHEN 'bronze' THEN 2
    WHEN 'silver' THEN 3
    WHEN 'gold' THEN 4
  END
""")

rows = layer_summary_df.collect()

print("=" * 75)
print("  PIPELINE LATENCY (ingestion → gold, from SDP event log)")
print("=" * 75)
print(f"  {'Layer':<12} {'Duration':>10} {'Cumulative':>12} {'Rows':>12} {'Rows/sec':>10} {'Tables':>8}")
print(f"  {'─'*12} {'─'*10} {'─'*12} {'─'*12} {'─'*10} {'─'*8}")
for r in rows:
    dur = r['layer_duration_sec'] or 0
    cum = r['cumulative_sec'] or 0
    rps = r['rows_per_sec'] or 0
    print(f"  {r['layer']:<12} {dur:>9.1f}s {cum:>11.1f}s {r['total_rows']:>12,} {rps:>10,.0f} {r['datasets_completed']:>8}")

if rows:
    total_dur = rows[-1]['cumulative_sec'] or 0
    total_rows = sum(r['total_rows'] or 0 for r in rows)
    print(f"  {'─'*12} {'─'*10} {'─'*12} {'─'*12} {'─'*10} {'─'*8}")
    print(f"  {'TOTAL':<12} {'':<10} {total_dur:>11.1f}s {total_rows:>12,}")
    print()
    print(f"  Batch SLA (<15 min):  {'PASS' if total_dur < 900 else 'FAIL'} ({total_dur:.0f}s / 900s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Metrics

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS waggoner_mom.prebronze._latency_metrics")
layer_summary_df.write.mode("overwrite").saveAsTable("waggoner_mom.prebronze._latency_metrics")
print("Logged to waggoner_mom.prebronze._latency_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Latency Waterfall & Throughput

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

if rows:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle("Medallion Pipeline: Latency & Throughput", fontsize=14, fontweight="bold")

    layers = [r['layer'] for r in rows]
    durations = [r['layer_duration_sec'] or 0 for r in rows]
    cumulatives = [r['cumulative_sec'] or 0 for r in rows]
    total_rows_list = [r['total_rows'] or 0 for r in rows]
    rps_list = [r['rows_per_sec'] or 0 for r in rows]
    bar_colors = {"prebronze": "#3b82f6", "bronze": "#8b5cf6", "silver": "#f59e0b", "gold": "#10b981"}
    colors = [bar_colors.get(l, "#999") for l in layers]

    # --- Left: Waterfall (cumulative latency) ---
    bottoms = [0] + cumulatives[:-1]
    bars = ax1.bar(layers, durations, bottom=bottoms, color=colors, width=0.5, edgecolor="white", linewidth=1)
    for i, (b, d, c) in enumerate(zip(bars, durations, cumulatives)):
        if d > 0:
            ax1.text(i, c + 1, f"+{d:.0f}s", ha="center", va="bottom", fontsize=10, fontweight="bold")

    total_dur = cumulatives[-1] if cumulatives else 0
    ax1.axhline(y=900, color="red", linestyle="--", alpha=0.5, linewidth=1.5)
    ax1.text(len(layers) - 0.5, 905, "SLA (15 min)", color="red", fontsize=9, ha="right")
    ax1.set_ylabel("Cumulative Seconds")
    sla_color = "#10b981" if total_dur < 900 else "#ef4444"
    ax1.set_title(f"Latency: {total_dur:.0f}s total  |  {'PASS' if total_dur < 900 else 'FAIL'}",
                  fontsize=11, color=sla_color)

    # --- Right: Throughput (rows/sec per layer) ---
    bars2 = ax2.bar(layers, rps_list, color=colors, width=0.5, edgecolor="white", linewidth=1)
    for i, (b, rps, total) in enumerate(zip(bars2, rps_list, total_rows_list)):
        ax2.text(i, rps + max(rps_list) * 0.02, f"{rps:,.0f}\n({total:,.0f} rows)",
                 ha="center", va="bottom", fontsize=9)
    ax2.set_ylabel("Rows / Second")
    ax2.set_title("Throughput per Layer", fontsize=11)
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"{x:,.0f}"))

    plt.tight_layout()
    plt.show()
else:
    print("No flow events found — run the pipeline first.")
