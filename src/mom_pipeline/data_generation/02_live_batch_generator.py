# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2, Task 1: Live Batch File Generator
# MAGIC Runs every 2 minutes. Generates incremental files for transaction-type sources.
# MAGIC Full-snapshot files (users, balances, linked accounts) are NOT regenerated — they arrive daily.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
import random

spark = SparkSession.builder.getOrCreate()
random.seed(int(datetime.now().timestamp()))

CATALOG = "waggoner_mom"
SCHEMA = "prebronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/partner_files"

# Load entity pool
entity_df = spark.read.table(f"{CATALOG}.{SCHEMA}._entity_ids")
entities = entity_df.collect()
account_ids = [r.account_id for r in entities]
card_ids = [r.card_id for r in entities]
user_ids = [r.user_id for r in entities]

now = datetime.now()
ts_suffix = now.strftime("%Y%m%d_%H%M%S")
date_prefix = now.strftime("%Y/%m/%d")

# COMMAND ----------

def write_csv_live(df, source_name):
    path = f"{VOLUME_PATH}/{source_name}/{date_prefix}/{ts_suffix}"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    return path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Incremental Files

# COMMAND ----------

merchants = ["Amazon", "Walmart", "Target", "Starbucks", "Shell", "Uber", "Netflix",
             "Spotify", "Apple", "Google", "DoorDash", "Costco"]

# Alerts (~28 rows per 2-min run)
alert_data = [(f"ALT-L-{ts_suffix}-{i}", random.choice(user_ids),
               random.choice(['fraud_alert', 'low_balance', 'large_transaction', 'login_attempt']),
               random.choice(['push', 'sms', 'email', 'in_app']),
               f"Alert generated at {now.isoformat()}",
               now, None,
               random.choice(['sent', 'delivered']))
              for i in range(28)]
alert_schema = StructType([
    StructField("alert_id", StringType()), StructField("user_id", StringType()),
    StructField("alert_type", StringType()), StructField("channel", StringType()),
    StructField("message", StringType()), StructField("created_at", TimestampType()),
    StructField("read_at", TimestampType(), True), StructField("status", StringType())
])
write_csv_live(spark.createDataFrame(alert_data, schema=alert_schema), "alerts")

# Payment Events (~20 rows)
pe_data = [(f"PE-L-{ts_suffix}-{i}", random.choice(card_ids),
            random.choice(['authorization', 'settlement', 'reversal']),
            round(random.uniform(1, 5000), 2),
            random.choice(merchants), now,
            random.choice(['approved', 'declined', 'pending']))
           for i in range(20)]
pe_schema = StructType([
    StructField("event_id", StringType()), StructField("card_id", StringType()),
    StructField("event_type", StringType()), StructField("amount", DoubleType()),
    StructField("merchant", StringType()), StructField("event_ts", TimestampType()),
    StructField("status", StringType())
])
write_csv_live(spark.createDataFrame(pe_data, schema=pe_schema), "payment_events")

# Card Payments (~8 rows)
cp_data = [(f"CP-L-{ts_suffix}-{i}", random.choice(card_ids),
            random.choice(merchants), round(random.uniform(1, 2000), 2),
            'USD', now,
            random.choice(['approved', 'declined']),
            random.choice(['5411', '5912', '5541', '5812']))
           for i in range(8)]
cp_schema = StructType([
    StructField("payment_id", StringType()), StructField("card_id", StringType()),
    StructField("merchant", StringType()), StructField("amount", DoubleType()),
    StructField("currency", StringType()), StructField("payment_date", TimestampType()),
    StructField("status", StringType()), StructField("mcc_code", StringType())
])
write_csv_live(spark.createDataFrame(cp_data, schema=cp_schema), "card_payments")

# ACH Payments (~2 rows)
ach_data = [(f"ACH-L-{ts_suffix}-{i}", random.choice(account_ids),
             round(random.uniform(5, 5000), 2), now.date(),
             random.choice(['inbound', 'outbound']),
             random.choice(['completed', 'pending']),
             random.choice(['PPD', 'CCD', 'WEB']))
            for i in range(2)]
ach_schema = StructType([
    StructField("payment_id", StringType()), StructField("account_id", StringType()),
    StructField("amount", DoubleType()), StructField("payment_date", DateType()),
    StructField("direction", StringType()), StructField("status", StringType()),
    StructField("ach_code", StringType())
])
write_csv_live(spark.createDataFrame(ach_data, schema=ach_schema), "ach_payments")

# Wire Transfers (~5 rows)
wr_data = [(f"WR-L-{ts_suffix}-{i}", random.choice(card_ids),
            f"Recipient_{random.randint(1,1000)}", round(random.uniform(100, 25000), 2),
            'USD', now,
            random.choice(['completed', 'pending']),
            random.choice(['visa_direct', 'swift', 'fedwire']))
           for i in range(5)]
wr_schema = StructType([
    StructField("transfer_id", StringType()), StructField("card_id", StringType()),
    StructField("recipient", StringType()), StructField("amount", DoubleType()),
    StructField("currency", StringType()), StructField("transfer_date", TimestampType()),
    StructField("status", StringType()), StructField("network", StringType())
])
write_csv_live(spark.createDataFrame(wr_data, schema=wr_schema), "wire_transfers")

# Settled Payments (~6 rows)
sp_data = [(f"SP-L-{ts_suffix}-{i}", random.choice(account_ids),
            round(random.uniform(5, 5000), 2), 'USD',
            now.date(), now.date(),
            random.choice(['ach', 'wire', 'card']))
           for i in range(6)]
sp_schema = StructType([
    StructField("payment_id", StringType()), StructField("account_id", StringType()),
    StructField("amount", DoubleType()), StructField("currency", StringType()),
    StructField("settle_date", DateType()), StructField("original_payment_date", DateType()),
    StructField("payment_method", StringType())
])
write_csv_live(spark.createDataFrame(sp_data, schema=sp_schema), "settled_payments")

# COMMAND ----------

print(f"Live batch files generated at {now.isoformat()}")
print(f"Files written to {VOLUME_PATH}/*//{date_prefix}/{ts_suffix}/")
