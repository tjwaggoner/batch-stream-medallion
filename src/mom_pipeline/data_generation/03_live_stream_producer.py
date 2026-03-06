# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2, Task 2: Live Streaming Event Producer
# MAGIC Runs continuously. Writes domain event JSON files to simulate Kafka streaming.
# MAGIC Each batch writes ~3-5 events. Runs in a loop with 2-second intervals.
# MAGIC
# MAGIC Stop this notebook to stop event generation.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
import random
import json
import time

spark = SparkSession.builder.getOrCreate()

CATALOG = "waggoner_mom"
SCHEMA = "prebronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/partner_files/domain_events"

# Load entity pool
entity_df = spark.read.table(f"{CATALOG}.{SCHEMA}._entity_ids")
entities = entity_df.collect()
account_ids = [r.account_id for r in entities]
card_ids = [r.card_id for r in entities]
user_ids = [r.user_id for r in entities]

event_types = ['payment.created', 'payment.settled', 'account.updated',
               'card.activated', 'alert.triggered']
entity_types = ['payment', 'account', 'card', 'alert']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event Generation Loop

# COMMAND ----------

batch_num = 0
total_events = 0
start_time = datetime.now()

print(f"Starting live event producer at {start_time.isoformat()}")
print(f"Writing to: {VOLUME_PATH}")
print("Press Cancel to stop.")

try:
    while True:
        now = datetime.now()
        batch_size = random.randint(3, 5)

        events = []
        for i in range(batch_size):
            entity_type = random.choice(entity_types)
            if entity_type == 'payment':
                entity_id = random.choice(account_ids)
            elif entity_type == 'card':
                entity_id = random.choice(card_ids)
            else:
                entity_id = random.choice(user_ids)

            events.append((
                f"EVT-LIVE-{batch_num}-{i}",
                entity_type,
                entity_id,
                random.choice(event_types),
                now,
                json.dumps({
                    "source": "live_producer",
                    "batch": batch_num,
                    "generated_at": now.isoformat()
                })
            ))

        schema = StructType([
            StructField("event_id", StringType()),
            StructField("entity_type", StringType()),
            StructField("entity_id", StringType()),
            StructField("event_type", StringType()),
            StructField("event_ts", TimestampType()),
            StructField("payload", StringType())
        ])

        date_prefix = now.strftime("%Y/%m/%d")
        ts_suffix = now.strftime("%H%M%S_%f")
        path = f"{VOLUME_PATH}/{date_prefix}/live_{ts_suffix}"

        df = spark.createDataFrame(events, schema=schema)
        df.coalesce(1).write.mode("overwrite").json(path)

        batch_num += 1
        total_events += batch_size

        if batch_num % 30 == 0:  # Status every ~60 seconds
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = total_events / elapsed if elapsed > 0 else 0
            print(f"Batch {batch_num}: {total_events} events total, {rate:.1f} events/sec")

        time.sleep(2)

except KeyboardInterrupt:
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nStopped after {batch_num} batches, {total_events} events, {elapsed:.0f}s")
