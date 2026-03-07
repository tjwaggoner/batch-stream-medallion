# Databricks notebook source
# MAGIC %md
# MAGIC # Wait for Pipeline Processing
# MAGIC Polls gold tables until newly generated data appears, or times out after 3 minutes.
# MAGIC Pipeline must be running in continuous mode.

# COMMAND ----------

import time
from datetime import datetime, timedelta

spark.sql("USE CATALOG waggoner_mom")

# Capture the count before new data was generated
gold_txn_count_before = spark.sql("SELECT COUNT(*) AS cnt FROM gold.gold_fact_daily_transactions").collect()[0]["cnt"]
prebronze_count_before = spark.sql("SELECT COUNT(*) AS cnt FROM prebronze.prebronze_alerts").collect()[0]["cnt"]

print(f"Gold txn rows before: {gold_txn_count_before}")
print(f"Prebronze alert rows before: {prebronze_count_before}")

# COMMAND ----------

MAX_WAIT_SEC = 180  # 3 minutes
POLL_INTERVAL_SEC = 10
start = datetime.now()
detected = False

print(f"Waiting up to {MAX_WAIT_SEC}s for pipeline to process new data...")
print(f"Pipeline must be running in continuous mode.")

while (datetime.now() - start).total_seconds() < MAX_WAIT_SEC:
    # Check if prebronze has new rows (files picked up by Auto Loader)
    prebronze_count_now = spark.sql("SELECT COUNT(*) AS cnt FROM prebronze.prebronze_alerts").collect()[0]["cnt"]

    if prebronze_count_now > prebronze_count_before:
        elapsed = (datetime.now() - start).total_seconds()
        new_rows = prebronze_count_now - prebronze_count_before
        print(f"New data detected in prebronze after {elapsed:.0f}s ({new_rows} new alert rows)")

        # Now wait a bit more for it to propagate through bronze → silver → gold
        print("Waiting 30s for propagation through bronze → silver → gold...")
        time.sleep(30)

        gold_txn_count_now = spark.sql("SELECT COUNT(*) AS cnt FROM gold.gold_fact_daily_transactions").collect()[0]["cnt"]
        print(f"Gold txn rows after: {gold_txn_count_now} (was {gold_txn_count_before})")
        detected = True
        break

    time.sleep(POLL_INTERVAL_SEC)

if not detected:
    elapsed = (datetime.now() - start).total_seconds()
    print(f"Timeout after {elapsed:.0f}s — no new data detected in prebronze.")
    print("Check that the pipeline is running in continuous mode:")
    print("  databricks bundle run mom_pipeline --profile e2-field")

# COMMAND ----------

print(f"Processing wait complete. Proceeding to latency check.")
