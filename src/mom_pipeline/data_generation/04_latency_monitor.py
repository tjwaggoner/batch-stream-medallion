# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Latency Monitor
# MAGIC Measures end-to-end latency from data generation through each layer to gold.
# MAGIC Logs results to `waggoner_mom.prebronze._latency_metrics`.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
spark.sql("USE CATALOG waggoner_mom")

now = datetime.now()
run_ts = now.isoformat()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measure Latency Per Layer

# COMMAND ----------

metrics = []

# Pre-bronze: latest ingested_at tells us when the most recent file was picked up
prebronze_latest = spark.sql("""
  SELECT MAX(_ingested_at) AS latest_ingested
  FROM prebronze.prebronze_alerts
""").collect()[0]["latest_ingested"]

# Bronze: latest _ingested_at after CDC merge
bronze_latest = spark.sql("""
  SELECT MAX(_ingested_at) AS latest_ingested
  FROM bronze.bronze_alerts
""").collect()[0]["latest_ingested"]

# Gold: use table history to get last refresh time
gold_txn_history = spark.sql("""
  DESCRIBE HISTORY waggoner_mom.gold.gold_fact_daily_transactions LIMIT 1
""").collect()
gold_last_refresh = gold_txn_history[0]["timestamp"] if gold_txn_history else None

# Domain events (streaming path): latest event_ts
domain_latest = spark.sql("""
  SELECT MAX(event_ts) AS latest_event
  FROM prebronze.prebronze_domain_events
""").collect()[0]["latest_event"]

domain_gold = spark.sql("""
  SELECT MAX(domain_event_ts) AS latest_domain
  FROM silver.silver_transactions
  WHERE domain_event_ts IS NOT NULL
""").collect()[0]["latest_domain"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Latencies

# COMMAND ----------

from datetime import timezone

def to_seconds(td):
    if td is None:
        return None
    return round(td.total_seconds(), 1)

# Batch path: file ingested_at → gold refresh
batch_prebronze_to_gold = None
if prebronze_latest and gold_last_refresh:
    if hasattr(prebronze_latest, 'tzinfo') and prebronze_latest.tzinfo:
        gold_ts = gold_last_refresh.replace(tzinfo=timezone.utc) if gold_last_refresh.tzinfo is None else gold_last_refresh
        batch_prebronze_to_gold = to_seconds(gold_ts - prebronze_latest)
    else:
        batch_prebronze_to_gold = to_seconds(gold_last_refresh - prebronze_latest)

# Streaming path: event_ts → silver arrival
stream_to_silver = None
if domain_latest and domain_gold:
    stream_to_silver = to_seconds(domain_gold - domain_latest)

# Layer-to-layer: prebronze → bronze
prebronze_to_bronze = None
if prebronze_latest and bronze_latest:
    prebronze_to_bronze = to_seconds(bronze_latest - prebronze_latest)

print(f"Run timestamp:           {run_ts}")
print(f"Latest prebronze alert:  {prebronze_latest}")
print(f"Latest bronze alert:     {bronze_latest}")
print(f"Gold last refresh:       {gold_last_refresh}")
print(f"Latest domain event:     {domain_latest}")
print(f"Domain event in silver:  {domain_gold}")
print()
print(f"--- Latency Results ---")
print(f"Prebronze → Bronze:      {prebronze_to_bronze}s")
print(f"Prebronze → Gold:        {batch_prebronze_to_gold}s")
print(f"Stream event → Silver:   {stream_to_silver}s")

# SLA check
BATCH_SLA_SEC = 15 * 60  # 15 minutes
STREAM_SLA_SEC = 5 * 60  # 5 minutes

batch_ok = batch_prebronze_to_gold is not None and batch_prebronze_to_gold < BATCH_SLA_SEC
stream_ok = stream_to_silver is not None and abs(stream_to_silver) < STREAM_SLA_SEC

print()
print(f"Batch SLA (<15 min):     {'PASS' if batch_ok else 'FAIL'} ({batch_prebronze_to_gold}s vs {BATCH_SLA_SEC}s)")
print(f"Stream SLA (<5 min):     {'PASS' if stream_ok else 'FAIL'} ({stream_to_silver}s vs {STREAM_SLA_SEC}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Metrics to Delta Table

# COMMAND ----------

metrics_data = [(
    run_ts,
    str(prebronze_latest),
    str(bronze_latest),
    str(gold_last_refresh),
    str(domain_latest),
    str(domain_gold),
    prebronze_to_bronze,
    batch_prebronze_to_gold,
    stream_to_silver,
    batch_ok,
    stream_ok
)]

metrics_schema = StructType([
    StructField("run_ts", StringType()),
    StructField("prebronze_latest", StringType()),
    StructField("bronze_latest", StringType()),
    StructField("gold_last_refresh", StringType()),
    StructField("domain_latest_event", StringType()),
    StructField("domain_in_silver", StringType()),
    StructField("prebronze_to_bronze_sec", DoubleType()),
    StructField("prebronze_to_gold_sec", DoubleType()),
    StructField("stream_to_silver_sec", DoubleType()),
    StructField("batch_sla_pass", BooleanType()),
    StructField("stream_sla_pass", BooleanType())
])

metrics_df = spark.createDataFrame(metrics_data, schema=metrics_schema)
metrics_df.write.mode("append").saveAsTable("waggoner_mom.prebronze._latency_metrics")

print(f"Metrics logged to waggoner_mom.prebronze._latency_metrics")
