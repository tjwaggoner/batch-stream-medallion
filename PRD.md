# PRD: FinServ Operational Data Model — Batch + Stream Medallion Pipeline

**Status:** Draft
**Author:** Tanner Waggoner
**Date:** 2026-03-06
**Workspace:** E2 Field (`e2-demo-field-eng.cloud.databricks.com`, profile: `e2-field`)

---

## 1. Objective

Demonstrate how Databricks replaces a complex multi-system data pipeline — spanning file ingestion frameworks, message brokers, legacy data warehouses, and standalone OLAP engines — with a **unified medallion architecture** using Spark Declarative Pipelines (SDP). The demo shows batch and streaming ingestion converging into a single governed lakehouse, eliminating architectural complexity while achieving near real-time data availability.

This addresses common pain points in fintech data architectures:
- **Achieve near real-time data** by eliminating intermediate hops through legacy data stores
- **Reduce architectural complexity** by consolidating ETL, streaming, warehousing, and OLAP into one platform
- **Unified batch + streaming** so downstream consumers (risk analysts, ML models, operations) get a single view of customer and transaction data without system lag

---

## 2. Demo Scope

### In Scope
- Streaming ingestion from a message bus (Kafka) via SDP streaming tables (Bronze)
- Batch ingestion from external S3 volume via Auto Loader / `read_files()` (Bronze)
- Silver layer: 3NF normalized tables with batch-stream join (unified view)
- Gold layer: Denormalized aggregation tables and metric views for analytics
- Full pipeline orchestrated as a single SDP pipeline with medallion architecture

### Out of Scope
- PII encryption/decryption
- Production message broker or file ingestion framework connectivity
- ML model training or feature store integration
- Production SLAs and disaster recovery

---

## 3. Architecture

```
┌──────────────────────────┐     ┌──────────────────────────────┐
│   Message Bus            │     │   External S3 Volume         │
│   (Kafka)                │     │   (Partner Data Files)       │
│                          │     │                              │
│   Domain service events  │     │   20+ file types, ~1K        │
│   from microservices     │     │   files/day, ~4 GB/day,      │
│   — volumes TBD          │     │   ~30M rows/day total        │
│                          │     │                              │
│                          │     │   Key files:                 │
│                          │     │   - Users (~10M rows/day)    │
│                          │     │   - Linked Accounts (~6M)    │
│                          │     │   - Balances (~5M)           │
│                          │     │   - Alerts (~2M)             │
│                          │     │   - Payment Events (~1.5M)   │
│                          │     │   - Transactions (~1.5M      │
│                          │     │     across 4 types)          │
│                          │     │   - + 14 smaller file types  │
└────────┬─────────────────┘     └────────┬─────────────────────┘
         │ streaming                       │ batch (Auto Loader)
         ▼                                 ▼
┌──────────────────────────────────────────────────────────┐
│              BRONZE LAYER (Raw)                          │
│  waggoner.mom_bronze                                     │
│  ┌─────────────────────┐  ┌────────────────────────────┐ │
│  │ Streaming (Kafka)   │  │ Batch (Auto Loader/S3)     │ │
│  │ - domain_events     │  │ - users                    │ │
│  │   (TBD volumes)     │  │ - linked_accounts          │ │
│  │                     │  │ - account_balances         │ │
│  │                     │  │ - alerts                   │ │
│  │                     │  │ - payment_events           │ │
│  │                     │  │ - wire_transfers           │ │
│  │                     │  │ - card_payments            │ │
│  │                     │  │ - ach_payments             │ │
│  │                     │  │ - settled_payments         │ │
│  │                     │  │ - card_profiles            │ │
│  │                     │  │ - verification_checks      │ │
│  │                     │  │ - portal_activity          │ │
│  │                     │  │ - risk_operations          │ │
│  │                     │  │ - case_management          │ │
│  │                     │  │ - rule_performance         │ │
│  └─────────────────────┘  └────────────────────────────┘ │
│  Append-only, metadata columns (_ingested_at,            │
│  _source_file, _source_type: batch|stream)               │
└─────────────────────────────┬────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│              SILVER LAYER (3NF)                          │
│  waggoner.mom_silver                                     │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Normalized entity tables (3NF):                    │  │
│  │ - users             (SCD Type 2, from users)       │  │
│  │ - accounts          (SCD Type 2, balances+linked)  │  │
│  │ - transactions      (batch+stream join, 4 payment  │  │
│  │                      types unified)                │  │
│  │ - cards             (card profiles + payment events)│  │
│  │ - alerts            (alerts + payment events)      │  │
│  │ - verifications     (verification checks + logins) │  │
│  │ - portal_activity   (portal usage + searches)      │  │
│  │ - risk_operations   (disputes + cases + rules)     │  │
│  └────────────────────────────────────────────────────┘  │
│  Cleaned, validated, deduplicated, typed.                │
│  Batch + stream unified via upsert/merge.                │
└─────────────────────────────┬────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│              GOLD LAYER (Analytics)                      │
│  waggoner.mom_gold                                       │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Denormalized / aggregation tables:                 │  │
│  │ - fact_daily_transactions                          │  │
│  │ - fact_user_activity                               │  │
│  │ - fact_risk_operations                             │  │
│  │ - dim_users                                        │  │
│  │ - dim_accounts                                     │  │
│  │ - dim_cards                                        │  │
│  │ - agg_risk_summary                                 │  │
│  │                                                    │  │
│  │ Metric Views:                                      │  │
│  │ - mv_transaction_kpis                              │  │
│  │ - mv_user_health                                   │  │
│  │ - mv_risk_operations                               │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## 4. Workspace & Catalog Configuration

| Setting | Value |
|---------|-------|
| **Workspace** | `e2-demo-field-eng.cloud.databricks.com` (profile: `e2-field`) |
| **Catalog** | `waggoner` (existing) |
| **Bronze Schema** | `mom_bronze` |
| **Silver Schema** | `mom_silver` |
| **Gold Schema** | `mom_gold` |
| **Volume (batch source)** | `/Volumes/waggoner/mom_bronze/partner_files/` |
| **Schema Metadata Volume** | `/Volumes/waggoner/mom_bronze/pipeline_metadata/schemas` |
| **Compute** | Serverless (SDP default) |

---

## 5. Data Model

### 5.1 Bronze Layer — Raw Ingestion

All bronze tables are **streaming tables** with append-only semantics.

#### Streaming Sources (Kafka)

These represent domain service events published from upstream microservices. Volumes are TBD — the partner file stats below do not cover streaming; these will be determined during integration.

| Table | Source Topic | Key Fields | Notes |
|-------|-------------|------------|-------|
| `bronze_domain_events` | `finserv.domain_events` | `event_id`, `entity_type`, `entity_id`, `event_ts` | Domain service CDC events; volumes TBD |

#### Batch Sources (Auto Loader from S3 Volume — Partner Data Files)

All file types below arrive as partner data files via a file ingestion framework. Files land in cloud storage and are processed as batch. Volumes represent a realistic fintech neobank scenario.

**High-Volume Files (>100K rows/day):**

| Table | Source Path | Format | Key Fields | Rows/Day | MB/Day | Files/Day |
|-------|-------------|--------|------------|----------|--------|-----------|
| `bronze_users` | `.../users/` | CSV | `user_id`, `full_name`, `email`, `signup_date` | 10,200,000 | 1,200 | 1 (full snapshot) |
| `bronze_linked_accounts` | `.../linked_accounts/` | CSV | `linked_acct_id`, `user_id`, `institution_name` | 6,100,000 | 400 | 1 (full snapshot) |
| `bronze_account_balances` | `.../account_balances/` | CSV | `account_id`, `balance`, `as_of_date` | 5,000,000 | 380 | 1 (full snapshot) |
| `bronze_alerts` | `.../alerts/` | CSV | `alert_id`, `user_id`, `alert_type`, `created_at` | 2,000,000 | 150 | 200 |
| `bronze_payment_events` | `.../payment_events/` | CSV | `event_id`, `card_id`, `event_type`, `event_ts` | 1,400,000 | 400 | 200 |
| `bronze_card_payments` | `.../card_payments/` | CSV | `payment_id`, `card_id`, `amount`, `payment_date` | 580,000 | 85 | 2 |
| `bronze_settled_payments` | `.../settled_payments/` | CSV | `payment_id`, `account_id`, `amount`, `settle_date` | 420,000 | 50 | 1 |
| `bronze_wire_transfers` | `.../wire_transfers/` | CSV | `transfer_id`, `card_id`, `recipient`, `amount`, `transfer_date` | 350,000 | 320 | 3 |
| `bronze_verification_checks` | `.../verification_checks/` | CSV | `check_id`, `user_id`, `check_type`, `result` | 140,000 | 40 | 50 |
| `bronze_portal_activity` | `.../portal_activity/` | CSV | `session_id`, `user_id`, `action`, `timestamp` | 130,000 | 2 | 96 |
| `bronze_ach_payments` | `.../ach_payments/` | CSV | `payment_id`, `account_id`, `amount`, `payment_date` | 80,000 | 8 | 24 |

**Low-Volume Files (<100K rows/day):**

| Table | Source Path | Format | Key Fields | Rows/Day | Files/Day |
|-------|-------------|--------|------------|----------|-----------|
| `bronze_card_profiles` | `.../card_profiles/` | JSON | `card_id`, `account_id`, `card_type`, `status` | 10,000 | 24 |
| `bronze_risk_operations` | `.../risk_ops/` | CSV | `op_id`, `card_id`, `op_type`, `op_date` | 9,000 | 1 |
| `bronze_rule_performance` | `.../rule_performance/` | CSV | `rule_id`, `accounts_affected`, `false_positive_rate` | 6,000 | 1 |
| `bronze_portal_logins` | `.../portal_logins/` | CSV | `user_id`, `login_ts`, `ip_address` | 1,000 | 96 |
| `bronze_portal_searches` | `.../portal_searches/` | CSV | `user_id`, `search_term`, `search_ts` | 1,200 | 96 |
| `bronze_dispute_records` | `.../disputes/` | CSV | `dispute_id`, `account_id`, `amount`, `status` | 1,000 | 1 |
| `bronze_verification_logins` | `.../verification_logins/` | CSV | `user_id`, `login_ts`, `status` | 600 | 50 |
| `bronze_dispute_status_changes` | `.../dispute_status/` | CSV | `dispute_id`, `old_status`, `new_status`, `change_ts` | 300 | 1 |
| `bronze_portal_users` | `.../portal_users/` | CSV | `user_id`, `role`, `status` | 20 | 96 |
| `bronze_rule_summaries` | `.../rule_summaries/` | CSV | `rule_id`, `summary_date`, `hit_count` | 10 | 1 |
| `bronze_case_records` | `.../cases/` | CSV | `case_id`, `case_type`, `status`, `outcome` | ~5 (combined) | <1 |

**Totals: ~26.5M rows/day, ~3.1 GB/day, ~850 files/day across 22 file types.**
**2-Year Backfill Estimate: ~19B rows, ~2.3 TB, ~620K files.**

All bronze tables include metadata columns:
- `_ingested_at` — `current_timestamp()`
- `_source_file` — `_metadata.file_path` (batch) or `NULL` (stream)
- `_source_type` — `'batch'` or `'stream'`

### 5.2 Silver Layer — Normalized (3NF)

Silver tables are **streaming tables with AUTO CDC** for deduplication and SCD tracking. This is the key layer where batch and streaming data are unified.

| Table | Sources | Join Logic | SCD | Key Columns |
|-------|---------|------------|-----|-------------|
| `silver_users` | `bronze_users` | Deduplicate on `user_id` | Type 2 | `user_id`, `full_name`, `email`, `status` |
| `silver_accounts` | `bronze_account_balances`, `bronze_linked_accounts` | Join on `account_id` / `user_id` | Type 2 | `account_id`, `user_id`, `balance`, `institution_name` |
| `silver_transactions` | `bronze_ach_payments` + `bronze_wire_transfers` + `bronze_card_payments` + `bronze_settled_payments` + `bronze_domain_events` (stream) | **Batch+Stream Join**: Unify 4 batch payment types (~1.4M rows/day combined) with streaming domain events. Enrich with card metadata via `card_id` join to `silver_cards`. | Type 1 | `payment_id`, `account_id`, `card_id`, `amount`, `payment_type`, `card_type` |
| `silver_cards` | `bronze_card_profiles`, `bronze_payment_events` | Join card master (~10K/day) with payment events (~1.4M/day) on `card_id` | Type 2 | `card_id`, `account_id`, `card_type`, `status`, `last_event` |
| `silver_alerts` | `bronze_alerts`, `bronze_payment_events` | Union alerts (~2M/day) and payment events (~1.4M/day), deduplicate | Type 1 | `alert_id`, `user_id`, `alert_type`, `channel`, `created_at` |
| `silver_verifications` | `bronze_verification_checks`, `bronze_verification_logins` | Join verification checks (~140K/day) with login activity (~600/day) on `user_id` | Type 2 | `user_id`, `check_type`, `result`, `last_login_ts` |
| `silver_portal_activity` | `bronze_portal_activity`, `bronze_portal_logins`, `bronze_portal_searches`, `bronze_portal_users` | Union portal activity streams, join with portal users on `user_id` | Type 1 | `activity_id`, `user_id`, `activity_type`, `timestamp` |
| `silver_risk_operations` | `bronze_dispute_records`, `bronze_dispute_status_changes`, `bronze_case_records`, `bronze_rule_performance`, `bronze_rule_summaries`, `bronze_risk_operations` | Union dispute/case/rule sources into normalized risk operations entity | Type 1 | `operation_id`, `operation_type`, `account_id`, `status`, `created_at` |

**Critical demo point — `silver_transactions`:** This table demonstrates the unified batch+stream join. Four separate batch payment file types (ACH: ~80K/day, Wire: ~350K/day, Card: ~580K/day, Settled: ~420K/day = **~1.4M total/day**) are unified with streaming domain events into a single transactions table. This eliminates the common pattern of maintaining separate batch and stream paths with manual reconciliation.

### 5.3 Gold Layer — Denormalized + Aggregations

Gold tables use **materialized views** for aggregations and **AUTO CDC** for denormalized dimension tables.

#### Fact Tables (Materialized Views)

| Table | Source | Grain | Measures |
|-------|--------|-------|----------|
| `gold_fact_daily_transactions` | `silver_transactions` | `payment_date`, `payment_type`, `account_id` | `txn_count`, `total_amount`, `avg_amount`, `max_amount` |
| `gold_fact_user_activity` | `silver_users` + `silver_transactions` + `silver_alerts` | `user_id`, `activity_date` | `txn_count`, `total_spend`, `alert_count` |
| `gold_fact_risk_operations` | `silver_risk_operations` + `silver_transactions` | `operation_date`, `operation_type` | `dispute_count`, `total_disputed_amount`, `avg_resolution_days`, `false_positive_rate` |
| `gold_agg_risk_summary` | `silver_transactions` + `silver_cards` + `silver_risk_operations` | `payment_date`, `card_type` | `high_value_txn_count`, `distinct_accounts`, `total_flagged_amount` |

#### Dimension Tables (AUTO CDC → SCD Type 2)

| Table | Source | Key | Tracked Columns |
|-------|--------|-----|-----------------|
| `gold_dim_users` | `silver_users` | `user_id` | `full_name`, `email`, `status` |
| `gold_dim_accounts` | `silver_accounts` | `account_id` | `balance`, `institution_name`, `status` |
| `gold_dim_cards` | `silver_cards` | `card_id` | `card_type`, `status`, `last_event` |

#### Metric Views

| Metric View | Source Table | Dimensions | Measures |
|-------------|-------------|------------|----------|
| `gold_mv_transaction_kpis` | `gold_fact_daily_transactions` | `Payment Date`, `Payment Type`, `Account ID` | `Total Transactions`, `Total Volume`, `Avg Transaction Value`, `Volume per Account` |
| `gold_mv_user_health` | `gold_fact_user_activity` | `Activity Month`, `User Segment` | `Active Users`, `Avg Spend per User`, `Churn Risk Count` |
| `gold_mv_risk_operations` | `gold_fact_risk_operations` | `Operation Month`, `Operation Type` | `Total Disputes`, `Total Disputed Amount`, `Avg Resolution Days`, `False Positive Rate` |

---

## 6. Pipeline Design

### 6.1 Technology Choices

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Framework** | Spark Declarative Pipelines (SDP) | Modern replacement for DLT, serverless by default |
| **Language** | SQL | Simpler for demo readability; all transforms are SQL-expressible |
| **Compute** | Serverless | No cluster management, auto-scaling |
| **Project Structure** | Databricks Asset Bundles (DABs) | Multi-environment support, `databricks pipelines init` |
| **Ingestion (batch)** | `read_files()` with `STREAM` | Auto Loader pattern via SQL |
| **Ingestion (stream)** | `read_kafka()` or simulated via files | Kafka integration or file-based simulation |
| **CDC** | AUTO CDC with SCD Type 1 & 2 | Built-in dedup and history tracking |
| **Metric Views** | Unity Catalog YAML metric views | Governed KPI definitions |
| **Clustering** | `CLUSTER BY` (Liquid Clustering) | Modern default, not `PARTITION BY` |

### 6.2 Pipeline Configuration

Single SDP pipeline with multi-schema output using pipeline parameters:

```yaml
# resources/mom_pipeline.pipeline.yml
resources:
  pipelines:
    mom_pipeline:
      name: "mom_medallion_pipeline"
      catalog: "waggoner"
      schema: "mom_bronze"  # default target = bronze
      configuration:
        silver_schema: "mom_silver"
        gold_schema: "mom_gold"
        source_volume: "/Volumes/waggoner/mom_bronze/partner_files"
        schema_location_base: "/Volumes/waggoner/mom_bronze/pipeline_metadata/schemas"
```

### 6.3 File Structure

```
batch-stream-medallion/
├── PRD.md                          # This document
├── databricks.yml                  # Asset Bundle config (dev/prod)
├── resources/
│   └── mom_pipeline.pipeline.yml   # Pipeline resource definition
└── src/
    └── mom_pipeline/
        └── transformations/
            ├── bronze/
            │   ├── bronze_domain_events.sql           # Kafka streaming
            │   ├── bronze_users.sql                    # Auto Loader batch (10.2M/day)
            │   ├── bronze_linked_accounts.sql          # Auto Loader batch (6.1M/day)
            │   ├── bronze_account_balances.sql         # Auto Loader batch (5M/day)
            │   ├── bronze_alerts.sql                   # Auto Loader batch (2M/day)
            │   ├── bronze_payment_events.sql           # Auto Loader batch (1.4M/day)
            │   ├── bronze_card_payments.sql            # Auto Loader batch (580K/day)
            │   ├── bronze_settled_payments.sql         # Auto Loader batch (420K/day)
            │   ├── bronze_wire_transfers.sql           # Auto Loader batch (350K/day)
            │   ├── bronze_verification_checks.sql      # Auto Loader batch (140K/day)
            │   ├── bronze_portal_activity.sql          # Auto Loader batch (130K/day)
            │   ├── bronze_ach_payments.sql             # Auto Loader batch (80K/day)
            │   ├── bronze_card_profiles.sql            # Auto Loader batch (10K/day)
            │   ├── bronze_risk_operations.sql          # Auto Loader batch (9K/day)
            │   ├── bronze_rule_performance.sql         # Auto Loader batch (6K/day)
            │   ├── bronze_disputes.sql                 # Auto Loader batch (disputes + status)
            │   ├── bronze_portal_users_logins.sql      # Auto Loader batch (logins, searches, users)
            │   ├── bronze_verification_logins.sql      # Auto Loader batch (600/day)
            │   ├── bronze_rule_summaries.sql           # Auto Loader batch (10/day)
            │   └── bronze_case_records.sql             # Auto Loader batch (<5/day)
            ├── silver/
            │   ├── silver_users.sql                # CDC from users
            │   ├── silver_accounts.sql             # Join balances + linked accounts
            │   ├── silver_transactions.sql         # Stream+batch join (4 payment types + domain events)
            │   ├── silver_cards.sql                # Join card profiles + payment events
            │   ├── silver_alerts.sql               # Union alerts + payment events
            │   ├── silver_verifications.sql        # Join checks + logins
            │   ├── silver_portal_activity.sql      # Union portal activity streams
            │   └── silver_risk_operations.sql      # Union disputes + cases + rules
            └── gold/
                ├── gold_fact_daily_transactions.sql
                ├── gold_fact_user_activity.sql
                ├── gold_fact_risk_operations.sql
                ├── gold_agg_risk_summary.sql
                ├── gold_dim_users.sql
                ├── gold_dim_accounts.sql
                ├── gold_dim_cards.sql
                ├── gold_mv_transaction_kpis.sql    # Metric view
                ├── gold_mv_user_health.sql         # Metric view
                └── gold_mv_risk_operations.sql     # Metric view
```

---

## 7. AI Dev Kit Implementation Notes

When building this pipeline with the AI Dev Kit Claude Code plugin, follow these practices:

### Project Initialization
- Use `databricks pipelines init` to scaffold the Asset Bundle project
- Select **SQL** as the language
- Set initial catalog to `waggoner`

### SDP Best Practices
- **Import**: Not needed for SQL pipelines
- **Streaming tables**: Use `CREATE OR REFRESH STREAMING TABLE` for all bronze and silver tables
- **Materialized views**: Use `CREATE OR REFRESH MATERIALIZED VIEW` for gold aggregations
- **Batch ingestion**: Use `FROM STREAM read_files(...)` (must include `STREAM` keyword for streaming tables)
- **Clustering**: Always use `CLUSTER BY`, never `PARTITION BY`
- **Table references**: Use unqualified names within the same schema; use `spark.conf.get()` equivalent (`${key}` in SQL) for cross-schema references
- **Modern defaults**: Serverless compute, Unity Catalog, raw `.sql` files (not notebooks)

### Multi-Schema Pattern
- Pipeline default catalog/schema = `waggoner.mom_bronze`
- Silver and gold schemas referenced via pipeline configuration parameters
- Silver tables use fully-qualified names: `${silver_schema}.silver_users`
- Gold tables use fully-qualified names: `${gold_schema}.gold_fact_daily_transactions`

### Metric Views
- Require **Databricks Runtime 17.2+**
- Define in SQL with `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML`
- All measures must be queried with `MEASURE()` function
- `SELECT *` is not supported on metric views

### Validation Checklist
- [ ] Language: SQL
- [ ] Compute: Serverless
- [ ] All tables use `CLUSTER BY` (not `PARTITION BY`)
- [ ] Bronze streaming tables use `FROM STREAM read_files(...)` for batch sources
- [ ] Silver tables implement AUTO CDC for deduplication
- [ ] Gold materialized views reference silver tables correctly
- [ ] Pipeline parameters defined for cross-schema references
- [ ] Metric views use version 1.1 YAML spec
- [ ] Asset Bundle validates: `databricks bundle validate`

---

## 8. Synthetic Data Generation

Since this is a demo, synthetic data will simulate realistic fintech volumes (scaled down ~100x). All batch sources are generated as files in the S3 volume; streaming is simulated via Kafka producer or file-based fallback.

**Batch Sources (Partner Data Files → S3 Volume):**

| Data Type | Real Volume/Day | Demo Volume | Generation Method |
|-----------|----------------|-------------|-------------------|
| Users | 10,200,000 rows | ~100K rows | CSV files in S3 volume |
| Linked Accounts | 6,100,000 rows | ~60K rows | CSV files in S3 volume |
| Account Balances | 5,000,000 rows | ~50K rows | CSV files in S3 volume |
| Alerts | 2,000,000 rows | ~20K rows | CSV files in S3 volume |
| Payment Events | 1,400,000 rows | ~14K rows | CSV files in S3 volume |
| Card Payments | 580,000 rows | ~6K rows | CSV files in S3 volume |
| Settled Payments | 420,000 rows | ~4K rows | CSV files in S3 volume |
| Wire Transfers | 350,000 rows | ~4K rows | CSV files in S3 volume |
| Verification Checks | 140,000 rows | ~1K rows | CSV files in S3 volume |
| Portal Activity | 130,000 rows | ~1K rows | CSV files in S3 volume |
| ACH Payments | 80,000 rows | ~1K rows | CSV files in S3 volume |
| Card Profiles | 10,000 rows | ~500 rows | JSON files in S3 volume |
| Risk/Dispute/Case/Rule | ~16,000 rows combined | ~500 rows | CSV files in S3 volume |

**Streaming Sources (Kafka):**

| Data Type | Real Volume/Day | Demo Volume | Generation Method |
|-----------|----------------|-------------|-------------------|
| Domain Events | TBD | ~5K rows | Kafka producer or file-based simulation |

Use the `databricks-synthetic-data-generation` AI Dev Kit skill with Faker for realistic data.

---

## 9. Demo Talking Points

1. **Platform Consolidation**: "This single SDP pipeline replaces a multi-system stack — file ingestion framework, message broker, legacy warehouse, and standalone OLAP engine — with one framework, one catalog, one governance model."

2. **Unified Batch + Stream**: "The `silver_transactions` table joins streaming domain events with batch partner files automatically. No separate reconciliation process needed."

3. **Near Real-Time**: "Streaming tables process events as they arrive. No hourly batch windows. The silver and gold layers update continuously."

4. **Governed Metrics**: "Metric views in Unity Catalog ensure everyone uses the same definition of 'Total Volume' or 'Active Users' — across dashboards, Genie, and ad-hoc SQL."

5. **Operational Simplicity**: "Serverless compute, no cluster management, built-in lineage and data quality. The pipeline is the entire data platform."

6. **OLAP Engine Replacement**: "The gold layer with materialized views and Photon-powered SQL warehouses delivers the same low-latency analytics a standalone OLAP engine provides — without a separate system to manage."

---

## 10. Success Criteria

- [ ] Pipeline deploys and runs successfully on E2 Field workspace
- [ ] Bronze tables ingest from both Kafka (stream) and S3 volume (batch)
- [ ] Silver `silver_transactions` demonstrates batch+stream join
- [ ] Gold aggregation tables refresh correctly from silver
- [ ] Metric views are queryable with `MEASURE()` syntax
- [ ] End-to-end lineage visible in Unity Catalog
- [ ] Demo can be presented in under 30 minutes
