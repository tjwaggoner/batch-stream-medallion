# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1: Historical Backfill
# MAGIC Generates 7 days of synthetic data for all batch sources.
# MAGIC Run once before demo to populate gold tables with meaningful aggregations.

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from faker import Faker
import random
import json
from datetime import datetime, timedelta
import os

spark = SparkSession.builder.getOrCreate()
fake = Faker()
Faker.seed(42)
random.seed(42)

CATALOG = "waggoner_mom"
SCHEMA = "prebronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/partner_files"
BACKFILL_DAYS = 7
BASE_DATE = datetime.now() - timedelta(days=BACKFILL_DAYS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Entity Pool
# MAGIC Shared IDs ensure referential integrity across all file types.

# COMMAND ----------

NUM_USERS = 10000
NUM_ACCOUNTS = 15000
NUM_CARDS = 8000

# Generate users
user_ids = [f"USR-{str(i).zfill(7)}" for i in range(1, NUM_USERS + 1)]
user_data = [(uid, fake.name(), fake.email(), fake.phone_number(),
              fake.date_between(start_date='-3y', end_date='-30d'),
              random.choice(['active', 'inactive', 'suspended', 'pending']),
              fake.street_address(), fake.city(), fake.state_abbr(), fake.zipcode())
             for uid in user_ids]

user_schema = StructType([
    StructField("user_id", StringType()), StructField("full_name", StringType()),
    StructField("email", StringType()), StructField("phone", StringType()),
    StructField("signup_date", DateType()), StructField("status", StringType()),
    StructField("address", StringType()), StructField("city", StringType()),
    StructField("state", StringType()), StructField("zip", StringType())
])
users_df = spark.createDataFrame(user_data, schema=user_schema)

# Generate accounts (1-2 per user)
account_data = []
account_id_list = []
for i in range(NUM_ACCOUNTS):
    aid = f"ACT-{str(i).zfill(7)}"
    uid = random.choice(user_ids)
    account_data.append((aid, uid))
    account_id_list.append(aid)

# Generate cards (0-2 per user)
card_data = []
card_id_list = []
for i in range(NUM_CARDS):
    cid = f"CRD-{str(i).zfill(7)}"
    uid = random.choice(user_ids)
    aid = random.choice(account_id_list)
    card_data.append((cid, aid, uid, random.choice(['debit', 'credit', 'prepaid']),
                      random.choice(['visa', 'mastercard', 'amex']),
                      str(random.randint(1000, 9999)),
                      f"{random.randint(1,12):02d}/{random.randint(26,30)}",
                      random.choice(['active', 'inactive', 'blocked']),
                      fake.date_between(start_date='-2y', end_date='-30d')))
    card_id_list.append(cid)

# Generate linked accounts
linked_data = []
for i, uid in enumerate(user_ids):
    num_linked = random.randint(1, 3)
    for j in range(num_linked):
        laid = f"LNK-{str(i * 3 + j).zfill(7)}"
        linked_data.append((laid, uid, fake.company() + " Bank",
                           random.choice(['checking', 'savings', 'investment']),
                           str(random.randint(100000000, 999999999)),
                           str(random.randint(1000, 9999)),
                           fake.date_between(start_date='-2y', end_date='-30d'),
                           random.choice(['active', 'inactive', 'pending'])))

# Save entity pool as Delta table
entity_schema = StructType([
    StructField("user_id", StringType()),
    StructField("account_id", StringType()),
    StructField("card_id", StringType())
])
entity_pool = []
for i in range(max(NUM_USERS, NUM_ACCOUNTS, NUM_CARDS)):
    entity_pool.append((
        user_ids[i % NUM_USERS],
        account_id_list[i % NUM_ACCOUNTS],
        card_id_list[i % NUM_CARDS]
    ))
entity_df = spark.createDataFrame(entity_pool, schema=entity_schema)
entity_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}._entity_ids")

print(f"Entity pool: {NUM_USERS} users, {NUM_ACCOUNTS} accounts, {NUM_CARDS} cards")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Helper Functions

# COMMAND ----------

def write_csv(df, path, num_files=1):
    """Write DataFrame as CSV files to volume."""
    df.coalesce(num_files).write.mode("overwrite").option("header", "true").csv(path)

def write_json(df, path, num_files=1):
    """Write DataFrame as JSON files to volume."""
    df.coalesce(num_files).write.mode("overwrite").json(path)

def date_path(source_name, dt):
    """Generate dated subdirectory path."""
    return f"{VOLUME_PATH}/{source_name}/{dt.strftime('%Y/%m/%d')}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Full-Snapshot Files (Users, Linked Accounts, Balances)
# MAGIC These arrive as complete daily snapshots.

# COMMAND ----------

for day_offset in range(BACKFILL_DAYS):
    dt = BASE_DATE + timedelta(days=day_offset)

    # Users — full snapshot each day (~100K rows demo scale)
    # Randomly mutate a few fields to simulate changes
    mutated_users = users_df.withColumn(
        "status",
        F.when(F.rand() < 0.02, F.lit("inactive")).otherwise(F.col("status"))
    )
    write_csv(mutated_users, date_path("users", dt))

    # Linked accounts — full snapshot
    linked_schema = StructType([
        StructField("linked_acct_id", StringType()), StructField("user_id", StringType()),
        StructField("institution_name", StringType()), StructField("account_type", StringType()),
        StructField("routing_number", StringType()), StructField("last_four", StringType()),
        StructField("linked_date", DateType()), StructField("status", StringType())
    ])
    linked_df = spark.createDataFrame(linked_data, schema=linked_schema)
    write_csv(linked_df, date_path("linked_accounts", dt))

    # Account balances — full snapshot with daily balance changes
    balance_data = [(aid, random.choice(user_ids),
                     round(random.uniform(10, 50000), 2),
                     round(random.uniform(10, 50000), 2),
                     dt.date(), 'USD',
                     random.choice(['checking', 'savings', 'investment']))
                    for aid in account_id_list]
    balance_schema = StructType([
        StructField("account_id", StringType()), StructField("user_id", StringType()),
        StructField("balance", DoubleType()), StructField("available_balance", DoubleType()),
        StructField("as_of_date", DateType()), StructField("currency", StringType()),
        StructField("account_type", StringType())
    ])
    balance_df = spark.createDataFrame(balance_data, schema=balance_schema)
    write_csv(balance_df, date_path("account_balances", dt))

print(f"Generated {BACKFILL_DAYS} days of full-snapshot files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate Incremental Transaction Files

# COMMAND ----------

merchants = ["Amazon", "Walmart", "Target", "Starbucks", "Shell", "Uber", "Netflix",
             "Spotify", "Apple", "Google", "DoorDash", "Costco", "Whole Foods"]
mcc_codes = ["5411", "5912", "5541", "5812", "4121", "5311", "5999"]

for day_offset in range(BACKFILL_DAYS):
    dt = BASE_DATE + timedelta(days=day_offset)

    # ACH Payments (~1K/day demo)
    ach_data = [(f"ACH-{day_offset}-{i}", random.choice(account_id_list),
                 round(random.uniform(5, 5000), 2), dt.date(),
                 random.choice(['inbound', 'outbound']),
                 random.choice(['completed', 'pending', 'failed']),
                 random.choice(['PPD', 'CCD', 'WEB']))
                for i in range(1000)]
    ach_schema = StructType([
        StructField("payment_id", StringType()), StructField("account_id", StringType()),
        StructField("amount", DoubleType()), StructField("payment_date", DateType()),
        StructField("direction", StringType()), StructField("status", StringType()),
        StructField("ach_code", StringType())
    ])
    write_csv(spark.createDataFrame(ach_data, schema=ach_schema),
              date_path("ach_payments", dt))

    # Card Payments (~6K/day demo)
    card_pay_data = [(f"CP-{day_offset}-{i}", random.choice(card_id_list),
                      random.choice(merchants), round(random.uniform(1, 2000), 2),
                      'USD', datetime(dt.year, dt.month, dt.day,
                                     random.randint(0,23), random.randint(0,59)),
                      random.choice(['approved', 'declined', 'pending']),
                      random.choice(mcc_codes))
                     for i in range(6000)]
    card_pay_schema = StructType([
        StructField("payment_id", StringType()), StructField("card_id", StringType()),
        StructField("merchant", StringType()), StructField("amount", DoubleType()),
        StructField("currency", StringType()), StructField("payment_date", TimestampType()),
        StructField("status", StringType()), StructField("mcc_code", StringType())
    ])
    write_csv(spark.createDataFrame(card_pay_data, schema=card_pay_schema),
              date_path("card_payments", dt), num_files=2)

    # Wire Transfers (~4K/day demo)
    wire_data = [(f"WR-{day_offset}-{i}", random.choice(card_id_list),
                  fake.name(), round(random.uniform(100, 25000), 2), 'USD',
                  datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)),
                  random.choice(['completed', 'pending', 'failed']),
                  random.choice(['visa_direct', 'swift', 'fedwire']))
                 for i in range(4000)]
    wire_schema = StructType([
        StructField("transfer_id", StringType()), StructField("card_id", StringType()),
        StructField("recipient", StringType()), StructField("amount", DoubleType()),
        StructField("currency", StringType()), StructField("transfer_date", TimestampType()),
        StructField("status", StringType()), StructField("network", StringType())
    ])
    write_csv(spark.createDataFrame(wire_data, schema=wire_schema),
              date_path("wire_transfers", dt), num_files=3)

    # Settled Payments (~4K/day demo)
    settled_data = [(f"SP-{day_offset}-{i}", random.choice(account_id_list),
                     round(random.uniform(5, 5000), 2), 'USD',
                     dt.date(), (dt - timedelta(days=random.randint(1,3))).date(),
                     random.choice(['ach', 'wire', 'card']))
                    for i in range(4000)]
    settled_schema = StructType([
        StructField("payment_id", StringType()), StructField("account_id", StringType()),
        StructField("amount", DoubleType()), StructField("currency", StringType()),
        StructField("settle_date", DateType()), StructField("original_payment_date", DateType()),
        StructField("payment_method", StringType())
    ])
    write_csv(spark.createDataFrame(settled_data, schema=settled_schema),
              date_path("settled_payments", dt))

print(f"Generated {BACKFILL_DAYS} days of transaction files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Incremental Event/Alert Files

# COMMAND ----------

alert_types = ['fraud_alert', 'low_balance', 'large_transaction', 'login_attempt',
               'payment_failed', 'card_blocked']
channels = ['push', 'sms', 'email', 'in_app']
event_types = ['authorization', 'settlement', 'reversal', 'chargeback', 'refund']

for day_offset in range(BACKFILL_DAYS):
    dt = BASE_DATE + timedelta(days=day_offset)

    # Alerts (~20K/day demo)
    alert_data = [(f"ALT-{day_offset}-{i}", random.choice(user_ids),
                   random.choice(alert_types), random.choice(channels),
                   fake.sentence(),
                   datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)),
                   datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)) if random.random() > 0.3 else None,
                   random.choice(['sent', 'delivered', 'read', 'failed']))
                  for i in range(20000)]
    alert_schema = StructType([
        StructField("alert_id", StringType()), StructField("user_id", StringType()),
        StructField("alert_type", StringType()), StructField("channel", StringType()),
        StructField("message", StringType()), StructField("created_at", TimestampType()),
        StructField("read_at", TimestampType(), True), StructField("status", StringType())
    ])
    write_csv(spark.createDataFrame(alert_data, schema=alert_schema),
              date_path("alerts", dt), num_files=4)

    # Payment Events (~14K/day demo)
    pay_event_data = [(f"PE-{day_offset}-{i}", random.choice(card_id_list),
                       random.choice(event_types), round(random.uniform(1, 5000), 2),
                       random.choice(merchants),
                       datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)),
                       random.choice(['approved', 'declined', 'pending']))
                      for i in range(14000)]
    pay_event_schema = StructType([
        StructField("event_id", StringType()), StructField("card_id", StringType()),
        StructField("event_type", StringType()), StructField("amount", DoubleType()),
        StructField("merchant", StringType()), StructField("event_ts", TimestampType()),
        StructField("status", StringType())
    ])
    write_csv(spark.createDataFrame(pay_event_data, schema=pay_event_schema),
              date_path("payment_events", dt), num_files=4)

print(f"Generated {BACKFILL_DAYS} days of event/alert files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Generate Low-Volume Files

# COMMAND ----------

for day_offset in range(BACKFILL_DAYS):
    dt = BASE_DATE + timedelta(days=day_offset)

    # Card Profiles (~500/day demo) — JSON format
    card_profile_data = [(cid, aid, uid, ctype, network, last4, expiry, status, issued)
                         for cid, aid, uid, ctype, network, last4, expiry, status, issued in card_data[:500]]
    card_profile_schema = StructType([
        StructField("card_id", StringType()), StructField("account_id", StringType()),
        StructField("user_id", StringType()), StructField("card_type", StringType()),
        StructField("network", StringType()), StructField("last_four", StringType()),
        StructField("expiry_date", StringType()), StructField("status", StringType()),
        StructField("issued_date", DateType())
    ])
    write_json(spark.createDataFrame(card_profile_data, schema=card_profile_schema),
               date_path("card_profiles", dt))

    # Verification Checks (~1K/day demo)
    check_data = [(f"CHK-{day_offset}-{i}", random.choice(user_ids),
                   random.choice(['identity', 'address', 'income', 'employment']),
                   random.choice(['pass', 'fail', 'review', 'pending']),
                   random.randint(0, 100),
                   datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)),
                   random.choice(['centrix', 'experian', 'equifax']))
                  for i in range(1000)]
    check_schema = StructType([
        StructField("check_id", StringType()), StructField("user_id", StringType()),
        StructField("check_type", StringType()), StructField("result", StringType()),
        StructField("score", IntegerType()), StructField("check_date", TimestampType()),
        StructField("provider", StringType())
    ])
    write_csv(spark.createDataFrame(check_data, schema=check_schema),
              date_path("verification_checks", dt))

    # Portal Activity (~1K/day demo)
    portal_data = [(f"SES-{day_offset}-{i}", random.choice(user_ids[:100]),
                    random.choice(['view_account', 'search', 'update_profile', 'download_report']),
                    random.choice(['/dashboard', '/accounts', '/transactions', '/settings']),
                    fake.ipv4(),
                    datetime(dt.year, dt.month, dt.day, random.randint(8,18), random.randint(0,59)),
                    random.randint(100, 30000))
                   for i in range(1000)]
    portal_schema = StructType([
        StructField("session_id", StringType()), StructField("user_id", StringType()),
        StructField("action", StringType()), StructField("page", StringType()),
        StructField("ip_address", StringType()), StructField("timestamp", TimestampType()),
        StructField("duration_ms", IntegerType())
    ])
    write_csv(spark.createDataFrame(portal_data, schema=portal_schema),
              date_path("portal_activity", dt))

    # Risk Operations (~500/day demo combined)
    risk_data = [(f"ROP-{day_offset}-{i}", random.choice(card_id_list),
                  random.choice(['fraud_review', 'block', 'unblock', 'investigation']),
                  datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)),
                  round(random.uniform(50, 10000), 2),
                  random.randint(0, 100),
                  random.choice(['approved', 'blocked', 'review', 'escalated']))
                 for i in range(200)]
    risk_schema = StructType([
        StructField("op_id", StringType()), StructField("card_id", StringType()),
        StructField("op_type", StringType()), StructField("op_date", TimestampType()),
        StructField("amount", DoubleType()), StructField("risk_score", IntegerType()),
        StructField("decision", StringType())
    ])
    write_csv(spark.createDataFrame(risk_data, schema=risk_schema),
              date_path("risk_ops", dt))

    # Dispute Records
    dispute_data = [(f"DSP-{day_offset}-{i}", random.choice(account_id_list),
                     f"CP-{random.randint(0,6)}-{random.randint(0,5999)}",
                     round(random.uniform(10, 5000), 2),
                     random.choice(['unauthorized', 'duplicate', 'wrong_amount', 'not_received']),
                     random.choice(['open', 'investigating', 'resolved', 'denied']),
                     datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)))
                    for i in range(150)]
    dispute_schema = StructType([
        StructField("dispute_id", StringType()), StructField("account_id", StringType()),
        StructField("transaction_id", StringType()), StructField("amount", DoubleType()),
        StructField("reason", StringType()), StructField("status", StringType()),
        StructField("created_at", TimestampType())
    ])
    write_csv(spark.createDataFrame(dispute_data, schema=dispute_schema),
              date_path("disputes", dt))

    # Dispute Status Changes
    dsc_data = [(f"DSP-{day_offset}-{random.randint(0,149)}",
                 random.choice(['open', 'investigating']),
                 random.choice(['investigating', 'resolved', 'denied']),
                 datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)),
                 f"agent_{random.randint(1,50)}",
                 random.choice(['evidence_reviewed', 'customer_contacted', 'bank_response']))
                for i in range(50)]
    dsc_schema = StructType([
        StructField("dispute_id", StringType()), StructField("old_status", StringType()),
        StructField("new_status", StringType()), StructField("change_ts", TimestampType()),
        StructField("changed_by", StringType()), StructField("reason", StringType())
    ])
    write_csv(spark.createDataFrame(dsc_data, schema=dsc_schema),
              date_path("dispute_status", dt))

    # Rule Performance
    rule_data = [(f"RULE-{i}", f"Rule_{i}_{random.choice(['velocity','amount','geo','device'])}",
                  random.randint(100, 10000),
                  round(random.uniform(0.01, 0.25), 4),
                  round(random.uniform(0.5, 0.99), 4),
                  dt.date())
                 for i in range(100)]
    rule_schema = StructType([
        StructField("rule_id", StringType()), StructField("rule_name", StringType()),
        StructField("accounts_affected", IntegerType()),
        StructField("false_positive_rate", DoubleType()),
        StructField("true_positive_rate", DoubleType()),
        StructField("evaluation_date", DateType())
    ])
    write_csv(spark.createDataFrame(rule_data, schema=rule_schema),
              date_path("rule_performance", dt))

    # Rule Summaries
    rule_sum_data = [(f"RULE-{i}", dt.date(), random.randint(0, 500),
                      random.randint(0, 100), random.randint(0, 50))
                     for i in range(10)]
    rule_sum_schema = StructType([
        StructField("rule_id", StringType()), StructField("summary_date", DateType()),
        StructField("hit_count", IntegerType()), StructField("block_count", IntegerType()),
        StructField("review_count", IntegerType())
    ])
    write_csv(spark.createDataFrame(rule_sum_data, schema=rule_sum_schema),
              date_path("rule_summaries", dt))

    # Portal Logins
    login_data = [(random.choice(user_ids[:100]),
                   datetime(dt.year, dt.month, dt.day, random.randint(6,22), random.randint(0,59)),
                   fake.ipv4(), fake.user_agent(),
                   random.random() > 0.05)
                  for i in range(150)]
    login_schema = StructType([
        StructField("user_id", StringType()), StructField("login_ts", TimestampType()),
        StructField("ip_address", StringType()), StructField("user_agent", StringType()),
        StructField("success", BooleanType())
    ])
    write_csv(spark.createDataFrame(login_data, schema=login_schema),
              date_path("portal_logins", dt))

    # Portal Searches
    search_data = [(random.choice(user_ids[:100]),
                    random.choice(['account lookup', 'transaction search', 'user query',
                                  'dispute check', 'card status']),
                    datetime(dt.year, dt.month, dt.day, random.randint(8,18), random.randint(0,59)),
                    random.randint(0, 50),
                    random.choice(['account', 'transaction', 'user', 'dispute']))
                   for i in range(180)]
    search_schema = StructType([
        StructField("user_id", StringType()), StructField("search_term", StringType()),
        StructField("search_ts", TimestampType()), StructField("results_count", IntegerType()),
        StructField("search_type", StringType())
    ])
    write_csv(spark.createDataFrame(search_data, schema=search_schema),
              date_path("portal_searches", dt))

    # Portal Users (small reference table)
    portal_user_data = [(f"ADMIN-{i}", f"admin_{i}",
                         random.choice(['admin', 'analyst', 'manager', 'viewer']),
                         random.choice(['operations', 'risk', 'compliance', 'support']),
                         'active',
                         fake.date_time_between(start_date='-2y', end_date='-6m'),
                         datetime(dt.year, dt.month, dt.day, random.randint(8,18), random.randint(0,59)))
                        for i in range(20)]
    portal_user_schema = StructType([
        StructField("user_id", StringType()), StructField("username", StringType()),
        StructField("role", StringType()), StructField("department", StringType()),
        StructField("status", StringType()), StructField("created_at", TimestampType()),
        StructField("last_active", TimestampType())
    ])
    write_csv(spark.createDataFrame(portal_user_data, schema=portal_user_schema),
              date_path("portal_users", dt))

    # Verification Logins
    vlogin_data = [(random.choice(user_ids),
                    datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)),
                    random.choice(['success', 'failed', 'locked']),
                    random.choice(['centrix', 'experian']),
                    random.randint(1000, 120000))
                   for i in range(100)]
    vlogin_schema = StructType([
        StructField("user_id", StringType()), StructField("login_ts", TimestampType()),
        StructField("status", StringType()), StructField("provider", StringType()),
        StructField("session_duration_ms", IntegerType())
    ])
    write_csv(spark.createDataFrame(vlogin_data, schema=vlogin_schema),
              date_path("verification_logins", dt))

    # Case Records (very low volume)
    case_data = [(f"CASE-{day_offset}-{i}", random.choice(['fraud', 'dispute_escalation', 'compliance']),
                  random.choice(account_id_list),
                  random.choice(['open', 'investigating', 'closed']),
                  random.choice(['resolved', 'unresolved', None]),
                  f"agent_{random.randint(1,20)}",
                  datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)),
                  datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59)) if random.random() > 0.5 else None)
                 for i in range(5)]
    case_schema = StructType([
        StructField("case_id", StringType()), StructField("case_type", StringType()),
        StructField("account_id", StringType()), StructField("status", StringType()),
        StructField("outcome", StringType(), True), StructField("assigned_to", StringType()),
        StructField("created_at", TimestampType()), StructField("closed_at", TimestampType(), True)
    ])
    write_csv(spark.createDataFrame(case_data, schema=case_schema),
              date_path("cases", dt))

print(f"Generated {BACKFILL_DAYS} days of low-volume files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Generate Streaming Backfill (Domain Events as JSON files)

# COMMAND ----------

event_types_domain = ['payment.created', 'payment.settled', 'account.updated',
                      'card.activated', 'alert.triggered']

for day_offset in range(BACKFILL_DAYS):
    dt = BASE_DATE + timedelta(days=day_offset)

    domain_data = [(f"EVT-{day_offset}-{i}",
                    random.choice(['payment', 'account', 'card', 'alert']),
                    random.choice(account_id_list + card_id_list),
                    random.choice(event_types_domain),
                    datetime(dt.year, dt.month, dt.day, random.randint(0,23),
                            random.randint(0,59), random.randint(0,59)),
                    json.dumps({"source": "backfill", "day": day_offset}))
                   for i in range(5000)]
    domain_schema = StructType([
        StructField("event_id", StringType()), StructField("entity_type", StringType()),
        StructField("entity_id", StringType()), StructField("event_type", StringType()),
        StructField("event_ts", TimestampType()), StructField("payload", StringType())
    ])
    write_json(spark.createDataFrame(domain_data, schema=domain_schema),
               date_path("domain_events", dt), num_files=5)

print(f"Generated {BACKFILL_DAYS} days of domain event files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backfill Complete
# MAGIC
# MAGIC Summary of generated data:
# MAGIC - 7 days x 22 batch file types + domain events
# MAGIC - Entity pool: 10K users, 15K accounts, 8K cards
# MAGIC - Stored at: /Volumes/waggoner_mom/prebronze/partner_files/

# COMMAND ----------

# Verify files were written
for source in ["users", "linked_accounts", "account_balances", "alerts", "payment_events",
               "card_payments", "wire_transfers", "settled_payments", "ach_payments",
               "card_profiles", "verification_checks", "portal_activity", "risk_ops",
               "disputes", "dispute_status", "rule_performance", "rule_summaries",
               "portal_logins", "portal_searches", "portal_users", "verification_logins",
               "cases", "domain_events"]:
    try:
        files = dbutils.fs.ls(f"{VOLUME_PATH}/{source}/")
        print(f"✓ {source}: {len(files)} date directories")
    except Exception as e:
        print(f"✗ {source}: {e}")
