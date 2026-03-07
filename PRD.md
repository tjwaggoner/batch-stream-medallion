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
- Pre-Bronze: Append-only raw ingestion from S3 volume — batch via Auto Loader, streaming simulated via JSON files (production path: Kafka)
- Bronze: Merge/upsert to current state via AUTO CDC (SCD Type 1) — deduplicated, one clean table per source
- Silver: 3NF normalized tables with static-stream join (batch bronze + streaming domain events unified). Two table types: AUTO CDC SCD2 for single-source entities, materialized views for multi-source joins.
- Gold: Denormalized fact and dimension tables (materialized views) + materialized metric views (created outside SDP pipeline)
- Genie Space for natural language querying of gold layer
- Demo job: data generation → pipeline refresh → latency monitoring in one triggered run
- Full pipeline orchestrated as a single SDP pipeline with medallion architecture via DABs

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
└────────┬─────────────────┘     └────────┬─────────────────────┘
         │ streaming                       │ batch (Auto Loader)
         ▼                                 ▼
┌──────────────────────────────────────────────────────────┐
│          PRE-BRONZE LAYER (Raw / Append-Only)            │
│  waggoner_mom.prebronze                                  │
│                                                          │
│  Immutable audit trail. Every file and event lands here  │
│  as-is. No dedup, no merge. Metadata columns added.     │
│                                                          │
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
└─────────────────────────────┬────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│          BRONZE LAYER (Merged / Current State)           │
│  waggoner_mom.bronze                                     │
│                                                          │
│  Merge/upsert from pre-bronze. Deduplicates full         │
│  snapshots, applies SCD Type 1. One clean current-state  │
│  table per source. Batch sources are static tables;      │
│  streaming stays as streaming table.                     │
│                                                          │
│  ┌─────────────────────┐  ┌────────────────────────────┐ │
│  │ Streaming           │  │ Merged (from batch)        │ │
│  │ - domain_events     │  │ - users                    │ │
│  │   (pass-through)    │  │ - linked_accounts          │ │
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
└─────────────────────────────┬────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│              SILVER LAYER (3NF / Conformed)              │
│  waggoner_mom.silver                                     │
│                                                          │
│  3NF normalized entity tables. Cross-source joins,       │
│  static-stream joins (merged bronze batch + streaming    │
│  domain events), data conforming, business typing.       │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │ AUTO CDC (SCD Type 2) — single-source entities:   │  │
│  │ - users             (history tracked)              │  │
│  │ - cards             (history tracked)              │  │
│  │                                                    │  │
│  │ Materialized Views — multi-source joins:           │  │
│  │ - accounts          (join: balances + linked)      │  │
│  │ - transactions      (static-stream join: 4 merged  │  │
│  │                      payment types + domain_events)│  │
│  │ - cards_enriched    (join: profiles + pay events)  │  │
│  │ - alerts            (join: alerts + pay events)    │  │
│  │ - verifications     (join: checks + logins)        │  │
│  │ - portal_activity   (join: usage + searches)       │  │
│  │ - risk_operations   (join: disputes+cases+rules)   │  │
│  └────────────────────────────────────────────────────┘  │
└─────────────────────────────┬────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│              GOLD LAYER (Analytics)                      │
│  waggoner_mom.gold                                       │
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
| **Catalog** | `waggoner_mom` (new) |
| **Pre-Bronze Schema** | `prebronze` |
| **Bronze Schema** | `bronze` |
| **Silver Schema** | `silver` |
| **Gold Schema** | `gold` |
| **Volume (batch source)** | `/Volumes/waggoner_mom/prebronze/partner_files/` |
| **Schema Metadata Volume** | `/Volumes/waggoner_mom/prebronze/pipeline_metadata/schemas` |
| **Compute** | Serverless (SDP default) |

---

## 5. Data Model

### 5.1 Pre-Bronze Layer — Raw / Append-Only Ingestion

All pre-bronze tables are **streaming tables** with append-only semantics. This layer is the **immutable audit trail** — every file delivery and every streaming event is preserved exactly as received. No deduplication, no merge, no transformation beyond metadata enrichment.

#### Streaming Sources (File-Based Simulation)

Domain service events are simulated via JSON files written to the S3 volume. In production, this would be replaced with `read_kafka()` for real Kafka ingestion. The demo uses Auto Loader (`read_files()`) on JSON files so the pipeline works without Kafka infrastructure.

| Table | Source Path | Format | Key Fields | Notes |
|-------|------------|--------|------------|-------|
| `prebronze_domain_events` | `.../domain_events/` | JSON | `event_id`, `entity_type`, `entity_id`, `event_ts` | Simulated domain events; ~5K/day demo |

#### Batch Sources (Auto Loader from S3 Volume — Partner Data Files)

All file types below arrive as partner data files via a file ingestion framework. Files land in cloud storage and are processed as batch. Volumes represent a realistic fintech neobank scenario.

**High-Volume Files (>100K rows/day):**

| Table | Source Path | Format | Key Fields | Rows/Day | MB/Day | Files/Day |
|-------|-------------|--------|------------|----------|--------|-----------|
| `prebronze_users` | `.../users/` | CSV | `user_id`, `full_name`, `email`, `signup_date` | 10,200,000 | 1,200 | 1 (full snapshot) |
| `prebronze_linked_accounts` | `.../linked_accounts/` | CSV | `linked_acct_id`, `user_id`, `institution_name` | 6,100,000 | 400 | 1 (full snapshot) |
| `prebronze_account_balances` | `.../account_balances/` | CSV | `account_id`, `balance`, `as_of_date` | 5,000,000 | 380 | 1 (full snapshot) |
| `prebronze_alerts` | `.../alerts/` | CSV | `alert_id`, `user_id`, `alert_type`, `created_at` | 2,000,000 | 150 | 200 |
| `prebronze_payment_events` | `.../payment_events/` | CSV | `event_id`, `card_id`, `event_type`, `event_ts` | 1,400,000 | 400 | 200 |
| `prebronze_card_payments` | `.../card_payments/` | CSV | `payment_id`, `card_id`, `amount`, `payment_date` | 580,000 | 85 | 2 |
| `prebronze_settled_payments` | `.../settled_payments/` | CSV | `payment_id`, `account_id`, `amount`, `settle_date` | 420,000 | 50 | 1 |
| `prebronze_wire_transfers` | `.../wire_transfers/` | CSV | `transfer_id`, `card_id`, `recipient`, `amount`, `transfer_date` | 350,000 | 320 | 3 |
| `prebronze_verification_checks` | `.../verification_checks/` | CSV | `check_id`, `user_id`, `check_type`, `result` | 140,000 | 40 | 50 |
| `prebronze_portal_activity` | `.../portal_activity/` | CSV | `session_id`, `user_id`, `action`, `timestamp` | 130,000 | 2 | 96 |
| `prebronze_ach_payments` | `.../ach_payments/` | CSV | `payment_id`, `account_id`, `amount`, `payment_date` | 80,000 | 8 | 24 |

**Low-Volume Files (<100K rows/day):**

| Table | Source Path | Format | Key Fields | Rows/Day | Files/Day |
|-------|-------------|--------|------------|----------|-----------|
| `prebronze_card_profiles` | `.../card_profiles/` | JSON | `card_id`, `account_id`, `card_type`, `status` | 10,000 | 24 |
| `prebronze_risk_operations` | `.../risk_ops/` | CSV | `op_id`, `card_id`, `op_type`, `op_date` | 9,000 | 1 |
| `prebronze_rule_performance` | `.../rule_performance/` | CSV | `rule_id`, `accounts_affected`, `false_positive_rate` | 6,000 | 1 |
| `prebronze_portal_logins` | `.../portal_logins/` | CSV | `user_id`, `login_ts`, `ip_address` | 1,000 | 96 |
| `prebronze_portal_searches` | `.../portal_searches/` | CSV | `user_id`, `search_term`, `search_ts` | 1,200 | 96 |
| `prebronze_dispute_records` | `.../disputes/` | CSV | `dispute_id`, `account_id`, `amount`, `status` | 1,000 | 1 |
| `prebronze_verification_logins` | `.../verification_logins/` | CSV | `user_id`, `login_ts`, `status` | 600 | 50 |
| `prebronze_dispute_status_changes` | `.../dispute_status/` | CSV | `dispute_id`, `old_status`, `new_status`, `change_ts` | 300 | 1 |
| `prebronze_portal_users` | `.../portal_users/` | CSV | `user_id`, `role`, `status` | 20 | 96 |
| `prebronze_rule_summaries` | `.../rule_summaries/` | CSV | `rule_id`, `summary_date`, `hit_count` | 10 | 1 |
| `prebronze_case_records` | `.../cases/` | CSV | `case_id`, `case_type`, `status`, `outcome` | ~5 (combined) | <1 |

**Totals: ~26.5M rows/day, ~3.1 GB/day, ~850 files/day across 22 file types.**
**2-Year Backfill Estimate: ~19B rows, ~2.3 TB, ~620K files.**

All pre-bronze tables include metadata columns:
- `_ingested_at` — `current_timestamp()`
- `_source_file` — `_metadata.file_path` (batch) or `NULL` (stream)
- `_source_type` — `'batch'` or `'stream'`
- `_rescued_data` — JSON string containing any columns not in the current schema (see below)

#### Schema Evolution Strategy

All Auto Loader batch sources use **`rescue` mode** (`cloudFiles.schemaEvolutionMode = "rescue"`). When a partner file arrives with new or unexpected columns, the pipeline continues running — the new columns are captured as JSON in a `_rescued_data` column rather than being added to the table schema automatically or failing the pipeline.

This approach fits the pre-bronze layer's role as an immutable audit trail:
- **No data is lost** — unexpected fields are preserved in `_rescued_data` and can be inspected
- **Pipeline stays running** — no manual intervention required when upstream schemas change
- **Schema changes are intentional** — new columns are promoted to the table schema only after review, by updating the `read_files()` schema hint in the SQL definition
- **Downstream layers are insulated** — bronze, silver, and gold are unaffected until the pre-bronze schema is explicitly updated

Schema location for Auto Loader inference is stored at: `/Volumes/waggoner_mom/prebronze/pipeline_metadata/schemas`

### 5.2 Bronze Layer — Merged / Current State

Bronze reads from pre-bronze and applies **AUTO CDC (SCD Type 1)** to merge changes into current-state tables. Full-snapshot files (users, linked accounts, balances) are deduplicated here — no duplicate rows propagate downstream. Incremental files are merged on their natural key.

The streaming source (`domain_events`) passes through as a streaming table since events are inherently append-only and don't need merge.

| Table | Source | Merge Key | Merge Strategy | Notes |
|-------|--------|-----------|----------------|-------|
| `bronze_users` | `prebronze_users` | `user_id` | SCD Type 1 (upsert) | Full daily snapshot → merged to current state |
| `bronze_linked_accounts` | `prebronze_linked_accounts` | `linked_acct_id` | SCD Type 1 (upsert) | Full daily snapshot → merged to current state |
| `bronze_account_balances` | `prebronze_account_balances` | `account_id` | SCD Type 1 (upsert) | Full daily snapshot → merged to current state |
| `bronze_alerts` | `prebronze_alerts` | `alert_id` | SCD Type 1 (upsert) | Incremental files, dedup on key |
| `bronze_payment_events` | `prebronze_payment_events` | `event_id` | SCD Type 1 (upsert) | Incremental files, dedup on key |
| `bronze_card_payments` | `prebronze_card_payments` | `payment_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_settled_payments` | `prebronze_settled_payments` | `payment_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_wire_transfers` | `prebronze_wire_transfers` | `transfer_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_ach_payments` | `prebronze_ach_payments` | `payment_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_card_profiles` | `prebronze_card_profiles` | `card_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_verification_checks` | `prebronze_verification_checks` | `check_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_portal_activity` | `prebronze_portal_activity` | `session_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_portal_logins` | `prebronze_portal_logins` | `user_id`, `login_ts` | SCD Type 1 (upsert) | Incremental |
| `bronze_portal_searches` | `prebronze_portal_searches` | `user_id`, `search_ts` | SCD Type 1 (upsert) | Incremental |
| `bronze_portal_users` | `prebronze_portal_users` | `user_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_risk_operations` | `prebronze_risk_operations` | `op_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_rule_performance` | `prebronze_rule_performance` | `rule_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_rule_summaries` | `prebronze_rule_summaries` | `rule_id`, `summary_date` | SCD Type 1 (upsert) | Incremental |
| `bronze_dispute_records` | `prebronze_dispute_records` | `dispute_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_dispute_status_changes` | `prebronze_dispute_status_changes` | `dispute_id`, `change_ts` | SCD Type 1 (upsert) | Incremental |
| `bronze_case_records` | `prebronze_case_records` | `case_id` | SCD Type 1 (upsert) | Incremental |
| `bronze_domain_events` | `prebronze_domain_events` | — | Streaming pass-through | No merge needed; events are append-only |

### 5.3 Silver Layer — 3NF / Conformed

Silver reads from bronze (merged current-state tables) and builds **3NF normalized entity tables**. This is where cross-source joins happen, data is conformed to standard business types, and the **static-stream join** unifies batch and streaming data.

Silver tables read from bronze batch tables as **static DataFrames** (already merged/current) and join with the streaming `bronze_domain_events` table. This is the classic Spark static-stream join pattern.

Silver tables fall into two categories based on how many bronze sources they read from:

**Single-source entity tables** use **AUTO CDC with SCD Type 2** to track history. AUTO CDC watches one source table for changes and automatically maintains versioned rows with `__START_AT` and `__END_AT` timestamps. This works well when there's a 1:1 relationship between a bronze table and a silver entity.

**Multi-source joined tables** use **materialized views** instead. AUTO CDC requires a single streaming source — it can't natively watch multiple tables for changes and figure out which side triggered an update. A materialized view solves this by recomputing the full joined result whenever any source table changes, so it always reflects the correct current state without needing to wire up change-tracking across multiple inputs.

| Table | Sources | Type | Logic | Key Columns |
|-------|---------|------|-------|-------------|
| `silver_users` | `bronze_users` | **AUTO CDC (SCD2)** | Single source. Conform types, validate email, standardize status codes. History tracked. | `user_id`, `full_name`, `email`, `status` |
| `silver_cards` | `bronze_card_profiles` | **AUTO CDC (SCD2)** | Single source. Conform card types. History tracked. | `card_id`, `account_id`, `card_type`, `status` |
| `silver_accounts` | `bronze_account_balances` + `bronze_linked_accounts` | **Materialized View** | Multi-source join on `account_id` / `user_id`. Conform institution names, standardize balance types. Recomputes on refresh. | `account_id`, `user_id`, `balance`, `institution_name` |
| `silver_transactions` | `bronze_ach_payments` + `bronze_wire_transfers` + `bronze_card_payments` + `bronze_settled_payments` (static) + `bronze_domain_events` (stream) | **Materialized View** | Multi-source. Union 4 merged payment tables (~1.4M rows/day), static-stream join with domain events, enrich with card metadata. Add conformed `payment_type`. | `payment_id`, `account_id`, `card_id`, `amount`, `payment_type`, `card_type` |
| `silver_cards_enriched` | `bronze_card_profiles` + `bronze_payment_events` | **Materialized View** | Multi-source join of card master with payment events on `card_id`. Last event timestamp derived. | `card_id`, `account_id`, `card_type`, `status`, `last_event` |
| `silver_alerts` | `bronze_alerts` + `bronze_payment_events` | **Materialized View** | Multi-source. Union and deduplicate. Conform alert types to standard taxonomy. | `alert_id`, `user_id`, `alert_type`, `channel`, `created_at` |
| `silver_verifications` | `bronze_verification_checks` + `bronze_verification_logins` | **Materialized View** | Multi-source join of checks with logins on `user_id`. Standardize check result codes. | `user_id`, `check_type`, `result`, `last_login_ts` |
| `silver_portal_activity` | `bronze_portal_activity` + `bronze_portal_logins` + `bronze_portal_searches` + `bronze_portal_users` | **Materialized View** | Multi-source. Union activity streams, join with portal users on `user_id`. Conform activity types. | `activity_id`, `user_id`, `activity_type`, `timestamp` |
| `silver_risk_operations` | `bronze_dispute_records` + `bronze_dispute_status_changes` + `bronze_case_records` + `bronze_rule_performance` + `bronze_rule_summaries` + `bronze_risk_operations` | **Materialized View** | Multi-source. Union all risk sources into normalized entity. Conform operation types and status codes. | `operation_id`, `operation_type`, `account_id`, `status`, `created_at` |

**Why this split matters:**
- **AUTO CDC (SCD2)** is ideal when you have one source feeding one entity — it incrementally processes only the changed rows and automatically maintains history versions. It's efficient and tracks exactly when each field changed.
- **Materialized views** are the right tool when combining multiple sources — they recompute the full result set on each refresh, which means changes from *any* source table are reflected without complex change-detection logic. The tradeoff is that materialized views don't track row-level history (no `__START_AT`/`__END_AT`), but the individual source entities that need history (users, cards) already have it via their own SCD2 tables.

**Point-in-time analysis:** If an analyst needs to answer "what did this account look like on March 1st?", they can query `silver_users` (SCD2, has history) directly. For joined entities like accounts, the current state is always available in the materialized view, and historical state can be reconstructed by joining `silver_users` history with the relevant bronze tables at a point in time.

**Critical demo point — `silver_transactions`:** This table demonstrates the **static-stream join**. The 4 payment type tables in bronze are already merged to current state (static), and are joined with the streaming `domain_events` table. This pattern — static batch data joined with a live stream — is the core of the unified ingestion design, eliminating the need for separate batch and stream processing paths.

### 5.4 Gold Layer — Denormalized + Aggregations

Gold tables use **materialized views** for aggregations and **AUTO CDC** for denormalized dimension tables.

#### Fact Tables (Materialized Views)

| Table | Source | Grain | Measures |
|-------|--------|-------|----------|
| `gold_fact_daily_transactions` | `silver_transactions` | `payment_date`, `payment_type`, `account_id` | `txn_count`, `total_amount`, `avg_amount`, `max_amount` |
| `gold_fact_user_activity` | `silver_users` + `silver_transactions` + `silver_alerts` | `user_id`, `activity_date` | `txn_count`, `total_spend`, `alert_count` |
| `gold_fact_risk_operations` | `silver_risk_operations` + `silver_transactions` | `operation_date`, `operation_type` | `dispute_count`, `total_disputed_amount`, `avg_resolution_days`, `false_positive_rate` |
| `gold_agg_risk_summary` | `silver_transactions` + `silver_cards` + `silver_risk_operations` | `payment_date`, `card_type` | `high_value_txn_count`, `distinct_accounts`, `total_flagged_amount` |

#### Dimension Tables (Materialized Views from Silver SCD2)

Gold dimension tables are **materialized views** that expose current state for dashboards and Genie. For silver SCD Type 2 sources (`silver_users`, `silver_cards`), they filter `WHERE __END_AT IS NULL` — full history remains queryable in silver for point-in-time analysis. For silver materialized view sources (`silver_accounts`), the data is already current state.

| Table | Source | Key | Columns |
|-------|--------|-----|---------|
| `gold_dim_users` | `silver_users` (SCD2, `WHERE __END_AT IS NULL`) | `user_id` | `full_name`, `email`, `status` |
| `gold_dim_accounts` | `silver_accounts` (materialized view, already current state) | `account_id` | `balance`, `institution_name`, `status` |
| `gold_dim_cards` | `silver_cards` (SCD2, `WHERE __END_AT IS NULL`) | `card_id` | `card_type`, `status` |

#### Materialized Metric Views

Metric views are **materialized** to pre-compute aggregations for fast query performance. Databricks automatically creates a managed Lakeflow SDP pipeline that refreshes the materializations on schedule. At query time, aggregate-aware query rewriting routes queries to the pre-computed views (fast path) or falls back to source data when materializations aren't available.

**Important:** Metric views are created **outside the SDP pipeline** via a separate notebook (`create_metric_views.py`). The `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML AS $$ ... $$` syntax is not supported inside SDP pipeline SQL files. The notebook must `USE CATALOG waggoner_mom` and `USE SCHEMA gold` before creating the views.

Each metric view YAML definition uses:
- `source`: Fully-qualified table name (e.g., `waggoner_mom.gold.gold_fact_daily_transactions`)
- `dimensions`: Each with `name` and `expr` (SQL expression, not `column`)
- `measures`: Each with `name` and `expr` (aggregate SQL expression like `SUM(amount)`)
- `materialization`: `schedule` (string like `every 2 hours`), `mode: relaxed`, and `materialized_views` list

| Metric View | Source Table | Dimensions | Measures | Materialization Type |
|-------------|-------------|------------|----------|---------------------|
| `gold_mv_transaction_kpis` | `gold_fact_daily_transactions` | `payment_date`, `payment_type`, `account_id` | `SUM(txn_count)`, `SUM(total_amount)`, `SUM(total_amount)/SUM(txn_count)` | `aggregated` |
| `gold_mv_user_health` | `gold_fact_user_activity` | `DATE_TRUNC('MONTH', activity_date)`, `user_id` | `COUNT(DISTINCT user_id)`, `AVG(total_spend)`, `SUM(alert_count)` | `aggregated` |
| `gold_mv_risk_operations` | `gold_fact_risk_operations` | `DATE_TRUNC('MONTH', operation_date)`, `operation_type` | `SUM(operation_count)`, `SUM(dispute_count)`, `SUM(total_disputed_amount)` | `aggregated` |

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
| **Ingestion (stream)** | `read_files()` on JSON (file-based simulation) | Simulates Kafka; swap to `read_kafka()` for production |
| **CDC** | AUTO CDC with SCD Type 1 & 2 | Built-in dedup and history tracking |
| **Metric Views** | Unity Catalog YAML metric views (materialized) | Governed KPI definitions with pre-computed aggregations |
| **Clustering** | `CLUSTER BY` (Liquid Clustering) | Modern default, not `PARTITION BY` |

### 6.2 Pipeline Configuration

Single SDP pipeline with multi-schema output using pipeline parameters:

```yaml
# resources/mom_pipeline.pipeline.yml
resources:
  pipelines:
    mom_pipeline:
      name: "mom_medallion_pipeline"
      catalog: "waggoner_mom"
      schema: "prebronze"  # default target = pre-bronze
      configuration:
        bronze_schema: "bronze"
        silver_schema: "silver"
        gold_schema: "gold"
        source_volume: "/Volumes/waggoner_mom/prebronze/partner_files"
        schema_location_base: "/Volumes/waggoner_mom/prebronze/pipeline_metadata/schemas"
      libraries:
        - glob:
            include: ../src/mom_pipeline/transformations/**
```

### 6.3 File Structure

```
batch-stream-medallion/
├── PRD.md                              # This document
├── README.md                           # Architecture overview and rationale
├── databricks.yml                      # Asset Bundle config (dev/prod)
├── resources/
│   ├── mom_pipeline.pipeline.yml       # SDP pipeline resource definition
│   └── mom_demo_job.job.yml            # Demo job: generate data → refresh → monitor
└── src/
    └── mom_pipeline/
        ├── transformations/            # SDP pipeline SQL (included via glob pattern)
        │   ├── prebronze/              # 23 streaming tables (Auto Loader + file-based stream)
        │   │   ├── prebronze_domain_events.sql       # File-based stream simulation (JSON)
        │   │   ├── prebronze_users.sql                # Auto Loader batch (10.2M/day)
        │   │   ├── prebronze_linked_accounts.sql      # Auto Loader batch (6.1M/day)
        │   │   ├── prebronze_account_balances.sql     # Auto Loader batch (5M/day)
        │   │   ├── prebronze_alerts.sql               # Auto Loader batch (2M/day)
        │   │   ├── prebronze_payment_events.sql       # Auto Loader batch (1.4M/day)
        │   │   ├── prebronze_card_payments.sql        # Auto Loader batch (580K/day)
        │   │   ├── prebronze_settled_payments.sql     # Auto Loader batch (420K/day)
        │   │   ├── prebronze_wire_transfers.sql       # Auto Loader batch (350K/day)
        │   │   ├── prebronze_verification_checks.sql  # Auto Loader batch (140K/day)
        │   │   ├── prebronze_portal_activity.sql      # Auto Loader batch (130K/day)
        │   │   ├── prebronze_ach_payments.sql         # Auto Loader batch (80K/day)
        │   │   ├── prebronze_card_profiles.sql        # Auto Loader batch (JSON, 10K/day)
        │   │   ├── prebronze_risk_operations.sql      # Auto Loader batch (9K/day)
        │   │   ├── prebronze_rule_performance.sql     # Auto Loader batch (6K/day)
        │   │   ├── prebronze_portal_logins.sql        # Auto Loader batch (1K/day)
        │   │   ├── prebronze_portal_searches.sql      # Auto Loader batch (1.2K/day)
        │   │   ├── prebronze_dispute_records.sql      # Auto Loader batch (1K/day)
        │   │   ├── prebronze_verification_logins.sql  # Auto Loader batch (600/day)
        │   │   ├── prebronze_dispute_status_changes.sql # Auto Loader batch (300/day)
        │   │   ├── prebronze_portal_users.sql         # Auto Loader batch (20/day)
        │   │   ├── prebronze_rule_summaries.sql       # Auto Loader batch (10/day)
        │   │   └── prebronze_case_records.sql         # Auto Loader batch (<5/day)
        │   ├── bronze/                 # 23 tables (22 AUTO CDC SCD1 + 1 streaming pass-through)
        │   │   ├── bronze_domain_events.sql           # Streaming pass-through
        │   │   ├── bronze_users.sql                   # AUTO CDC merge
        │   │   ├── bronze_linked_accounts.sql
        │   │   ├── bronze_account_balances.sql
        │   │   ├── bronze_alerts.sql
        │   │   ├── bronze_payment_events.sql
        │   │   ├── bronze_card_payments.sql
        │   │   ├── bronze_settled_payments.sql
        │   │   ├── bronze_wire_transfers.sql
        │   │   ├── bronze_ach_payments.sql
        │   │   ├── bronze_card_profiles.sql
        │   │   ├── bronze_verification_checks.sql
        │   │   ├── bronze_verification_logins.sql
        │   │   ├── bronze_portal_activity.sql
        │   │   ├── bronze_portal_logins.sql
        │   │   ├── bronze_portal_searches.sql
        │   │   ├── bronze_portal_users.sql
        │   │   ├── bronze_risk_operations.sql
        │   │   ├── bronze_rule_performance.sql
        │   │   ├── bronze_rule_summaries.sql
        │   │   ├── bronze_dispute_records.sql
        │   │   ├── bronze_dispute_status_changes.sql
        │   │   └── bronze_case_records.sql
        │   ├── silver/                 # 9 tables (2 AUTO CDC SCD2 + 7 materialized views)
        │   │   ├── silver_users.sql               # AUTO CDC SCD2
        │   │   ├── silver_cards.sql               # AUTO CDC SCD2
        │   │   ├── silver_accounts.sql            # MV (join: balances + linked)
        │   │   ├── silver_transactions.sql        # MV (static-stream join)
        │   │   ├── silver_cards_enriched.sql      # MV (join: profiles + events)
        │   │   ├── silver_alerts.sql              # MV (union: alerts + events)
        │   │   ├── silver_verifications.sql       # MV (join: checks + logins)
        │   │   ├── silver_portal_activity.sql     # MV (union: portal streams)
        │   │   └── silver_risk_operations.sql     # MV (union: disputes+cases+rules)
        │   └── gold/                   # 7 materialized views (4 facts + 3 dims)
        │       ├── gold_fact_daily_transactions.sql
        │       ├── gold_fact_user_activity.sql
        │       ├── gold_fact_risk_operations.sql
        │       ├── gold_agg_risk_summary.sql
        │       ├── gold_dim_users.sql
        │       ├── gold_dim_accounts.sql
        │       └── gold_dim_cards.sql
        ├── metric_views/               # Created outside SDP (separate notebook)
        │   ├── create_metric_views.py  # Notebook that creates all 3 metric views
        │   ├── gold_mv_transaction_kpis.sql   # Reference SQL
        │   ├── gold_mv_user_health.sql        # Reference SQL
        │   └── gold_mv_risk_operations.sql    # Reference SQL
        └── data_generation/            # Synthetic data notebooks
            ├── 01_backfill.py          # Phase 1: 7-day historical backfill
            ├── 02_live_batch_generator.py   # Phase 2: incremental batch files
            ├── 03_live_stream_producer.py   # Phase 2: domain event producer (90s loop)
            └── 04_latency_monitor.py   # SLA monitoring with waterfall chart
```

---

## 7. AI Dev Kit Implementation Notes

When building this pipeline with the AI Dev Kit Claude Code plugin, follow these practices:

### Project Initialization
- Use `databricks pipelines init` to scaffold the Asset Bundle project
- Select **SQL** as the language
- Set initial catalog to `waggoner_mom`

### SDP Best Practices
- **Import**: Not needed for SQL pipelines
- **Streaming tables**: Use `CREATE OR REFRESH STREAMING TABLE` for all bronze and silver tables
- **Materialized views**: Use `CREATE OR REFRESH MATERIALIZED VIEW` for gold aggregations
- **Batch ingestion**: Use `FROM STREAM read_files(...)` (must include `STREAM` keyword for streaming tables)
- **Clustering**: Always use `CLUSTER BY`, never `PARTITION BY`
- **Table references**: Use unqualified names within the same schema; use `spark.conf.get()` equivalent (`${key}` in SQL) for cross-schema references
- **Modern defaults**: Serverless compute, Unity Catalog, raw `.sql` files (not notebooks)

### Multi-Schema Pattern
- Pipeline default catalog/schema = `waggoner_mom.prebronze`
- Bronze, silver, and gold schemas referenced via pipeline configuration parameters
- Bronze tables use: `${bronze_schema}.bronze_users`
- Silver tables use: `${silver_schema}.silver_users`
- Gold tables use: `${gold_schema}.gold_fact_daily_transactions`

### Metric Views (Materialized)
- Require **Databricks Runtime 17.2+** and **serverless compute**
- **Created outside SDP pipeline** — `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML AS $$ ... $$` is not parseable inside SDP SQL files
- Created via `create_metric_views.py` notebook, which runs `USE CATALOG waggoner_mom` then `spark.sql()` for each view
- YAML uses `source: catalog.schema.table` (string), `expr` for dimensions/measures (SQL expressions), `materialization.schedule` as string (e.g., `every 2 hours`)
- Databricks auto-creates a managed SDP pipeline for refreshing materializations
- Queries automatically route to pre-computed views (fast path) when available
- All measures must be queried with `MEASURE()` function
- `SELECT *` is not supported on metric views

### Validation Checklist
- [ ] Language: SQL
- [ ] Compute: Serverless
- [ ] All tables use `CLUSTER BY` (not `PARTITION BY`)
- [ ] Pre-bronze streaming tables use `FROM STREAM read_files(...)` for batch sources
- [ ] Auto Loader uses `rescue` schema evolution mode with `_rescued_data` column
- [ ] Bronze tables implement AUTO CDC (SCD Type 1) merge from pre-bronze
- [ ] Silver tables implement cross-source joins and static-stream join
- [ ] Gold materialized views reference silver tables correctly
- [ ] Pipeline parameters defined for cross-schema references
- [ ] Metric views use version 1.1 YAML spec with `materialization` block
- [ ] Metric view materializations refresh on schedule and queries use fast path
- [ ] Asset Bundle validates: `databricks bundle validate`

---

## 8. Synthetic Data Generation

Since this is a demo, synthetic data simulates realistic fintech volumes (scaled down ~100x). Data generation runs in **two phases** to ensure gold tables are populated before the demo and data is flowing live to demonstrate SLA compliance.

### 8.1 Phase 1: Historical Backfill (One-Shot)

A one-time backfill script generates **7 days of historical data** so gold tables have meaningful aggregations when the demo starts. Runs once before the demo, not during.

**Batch files are generated as dated subdirectories** in the S3 volume to simulate how partner files would have arrived over the past week. Auto Loader processes them in order on first pipeline run.

| Data Type | Real Volume/Day | Demo Volume/Day | Backfill (7 days) | Format |
|-----------|----------------|-----------------|-------------------|--------|
| Users | 10,200,000 | ~100K | ~700K | CSV (full snapshot per day) |
| Linked Accounts | 6,100,000 | ~60K | ~420K | CSV (full snapshot per day) |
| Account Balances | 5,000,000 | ~50K | ~350K | CSV (full snapshot per day) |
| Alerts | 2,000,000 | ~20K | ~140K | CSV (incremental) |
| Payment Events | 1,400,000 | ~14K | ~98K | CSV (incremental) |
| Card Payments | 580,000 | ~6K | ~42K | CSV (incremental) |
| Settled Payments | 420,000 | ~4K | ~28K | CSV (incremental) |
| Wire Transfers | 350,000 | ~4K | ~28K | CSV (incremental) |
| Verification Checks | 140,000 | ~1K | ~7K | CSV (incremental) |
| Portal Activity | 130,000 | ~1K | ~7K | CSV (incremental) |
| ACH Payments | 80,000 | ~1K | ~7K | CSV (incremental) |
| Card Profiles | 10,000 | ~500 | ~3.5K | JSON (incremental) |
| Risk/Dispute/Case/Rule | ~16,000 combined | ~500 | ~3.5K | CSV (incremental) |

**Streaming backfill:** ~5K domain events per day × 7 days = ~35K events written as JSON files to a simulation volume (or produced to Kafka if available).

**Referential integrity:** Backfill generates a shared pool of entity IDs (user_id, account_id, card_id) first, then all file types reference from that pool. This ensures silver joins actually match across sources.

**Backfill totals: ~1.8M rows across all file types, ~200 MB.**

### 8.2 Phase 2: Live Generator (Continuous During Demo)

A **Databricks job (`mom_demo_live_data`)** with four tasks runs on-demand during the demo to generate fresh data, refresh the pipeline, and verify SLA compliance:

```
generate_batch_files ──┐
                       ├──→ refresh_pipeline ──→ check_latency
generate_stream_events ┘
```

Both generators run **in parallel** first, then the pipeline refreshes to process all new data (batch + stream files), then the latency monitor measures end-to-end time from generation to gold.

#### Task 1: Batch File Generator (runs every 2 minutes)

A scheduled notebook that generates new partner files and writes them to the S3 volume. Each run creates a small batch of files simulating realistic arrival patterns:

| Data Type | Rows per Run (every 2 min) | Files per Run | Notes |
|-----------|---------------------------|---------------|-------|
| Alerts | ~28 | 1 | Incremental |
| Payment Events | ~20 | 1 | Incremental |
| Card Payments | ~8 | 1 | Incremental |
| ACH Payments | ~2 | 1 | Incremental |
| Wire Transfers | ~5 | 1 | Incremental |
| Settled Payments | ~6 | 1 | Incremental |

Full-snapshot files (users, balances, linked accounts) are **not regenerated** during live demo — they arrive daily, so backfill covers them. Only incremental file types get new files during the demo.

#### Task 2: Streaming Event Producer (runs for 90 seconds)

A notebook that writes domain event JSON files to the S3 volume for 90 seconds, simulating streaming ingestion. Exits cleanly within the job timeout.

| Setting | Value |
|---------|-------|
| **Event rate** | ~3-5 events/second |
| **Event types** | `payment.created`, `payment.settled`, `account.updated`, `card.activated`, `alert.triggered` |
| **Entity references** | Draws from the same ID pool as backfill for join consistency |
| **Timestamp** | `event_ts` = `current_timestamp()` at generation time — used to measure end-to-end latency |

#### Task 3: Pipeline Refresh

The SDP pipeline is triggered automatically after both generators complete, processing the new files through all 4 layers.

#### Task 4: Latency Monitor (`04_latency_monitor.py`)

Measures hop-by-hop latency across the medallion layers using `unix_timestamp()` comparisons in SQL:

| Metric | Measurement | Target |
|--------|-------------|--------|
| **Prebronze → Bronze** | `MAX(_ingested_at)` in bronze - `MAX(_ingested_at)` in prebronze | — |
| **Bronze → Gold** | `current_timestamp()` (post-refresh) - `MAX(_ingested_at)` in bronze | — |
| **Total Batch** | Gold refresh time - prebronze ingestion time | < 15 minutes (900s) |
| **Total Stream** | Silver domain_event_ts - prebronze event_ts | < 5 minutes (300s) |

Results are logged to `waggoner_mom.prebronze._latency_metrics` and displayed as a waterfall chart showing cumulative latency at each layer with SLA threshold lines.

The demo job is triggered manually: `databricks bundle run mom_demo_job --profile e2-field`

### 8.3 Implementation

| Component | Implementation | Notes |
|-----------|---------------|-------|
| **Backfill script** | `01_backfill.py` — Python notebook using Faker + Spark | Generates 7 days of data, creates entity pool |
| **Live batch generator** | `02_live_batch_generator.py` — Python notebook | Writes incremental CSV files to S3 volume |
| **Live stream producer** | `03_live_stream_producer.py` — Python notebook (90s loop) | Writes JSON event files to volume (simulates Kafka) |
| **Latency monitor** | `04_latency_monitor.py` — Python notebook | Hop-by-hop latency measurement with waterfall chart |
| **Demo job** | `mom_demo_job.job.yml` — DABs job resource | 4-task chain: generate → refresh → monitor |
| **Metric views** | `create_metric_views.py` — separate notebook | Created outside SDP pipeline |
| **Shared ID pool** | Delta table `waggoner_mom.prebronze._entity_ids` | Generated once during backfill, reused by live generators |
| **Pipeline mode** | Triggered per demo job run | Each run generates data, refreshes pipeline, checks SLA |

### 8.4 Data Relationships

To ensure silver joins work correctly, all generators share a common entity graph:

```
users (10K unique) ──┬── linked_accounts (1-3 per user)
                     ├── account_balances (1 per linked account)
                     ├── cards (0-2 per user) ──── card_payments, wire_transfers
                     ├── alerts (random)
                     └── verification_checks (random)

Domain events reference: user_id, account_id, card_id from the same pools
Payment files reference: account_id, card_id from the same pools
```

This entity graph is generated once during backfill Phase 1 and reused by the live generator in Phase 2.

---

## 9. Genie Space — Natural Language Query Interface

A **Databricks Genie Space** will be created on top of the gold layer to enable non-technical users (analysts, operations staff, business stakeholders) to query transaction, user, and risk data using natural language — no SQL required.

### Configuration

| Setting | Value |
|---------|-------|
| **Genie Space Name** | `MOM Analytics` |
| **Source Tables** | All `gold` tables and metric views |
| **Warehouse** | Serverless SQL warehouse |

### Included Tables

| Table / View | Purpose | Example Questions |
|-------------|---------|-------------------|
| `gold_fact_daily_transactions` | Transaction volume and trends | "What was total payment volume last week?" |
| `gold_fact_user_activity` | User engagement metrics | "How many active users did we have in February?" |
| `gold_fact_risk_operations` | Dispute and case tracking | "Show me open disputes by type this month" |
| `gold_agg_risk_summary` | Risk overview by card type | "Which card types have the most flagged transactions?" |
| `gold_dim_users` | User lookups | "How many users signed up in the last 30 days?" |
| `gold_dim_accounts` | Account details | "What's the average account balance by institution?" |
| `gold_dim_cards` | Card inventory | "How many active cards do we have by card type?" |
| `gold_mv_transaction_kpis` | Governed transaction KPIs | "What's the average transaction value by payment type?" |
| `gold_mv_user_health` | User health metrics | "How many users are at churn risk?" |
| `gold_mv_risk_operations` | Risk operation KPIs | "What's the average dispute resolution time?" |

### Instructions for Genie

The Genie Space will include curated instructions to help the model understand domain context:

- **Payment types**: ACH, Wire, Card, Settled — represent different transaction channels
- **Risk operations**: Include disputes, cases, and rule evaluations
- **User segments**: Derived from activity patterns in `gold_fact_user_activity`
- **Metric views**: When a question aligns with a metric view, prefer querying it over the underlying fact table to ensure governed definitions are used
- **Time context**: Default to the most recent complete month unless a specific date range is provided

### Sample Certified Questions

Pre-configured questions to guide users and validate accuracy:

1. "What was total transaction volume by payment type this month?"
2. "How many new users signed up in the last 7 days?"
3. "Show me the top 10 accounts by total spend"
4. "What's the false positive rate for risk rules this month?"
5. "How does this week's payment volume compare to last week?"

---

## 10. Success Criteria

- [x] Pipeline deploys and runs successfully on E2 Field workspace
- [x] Pre-bronze tables ingest from file-based stream simulation and S3 volume (batch)
- [x] Bronze tables merge pre-bronze to current state via AUTO CDC (SCD Type 1)
- [x] Silver `silver_transactions` demonstrates static-stream join (4 payment types + domain events)
- [x] Silver `silver_users` and `silver_cards` track history via AUTO CDC (SCD Type 2)
- [x] Gold aggregation tables refresh correctly from silver
- [x] Materialized metric views created and queryable with `MEASURE()` syntax
- [x] Demo job generates data, refreshes pipeline, and monitors latency in one run
- [x] Latency monitor measures hop-by-hop latency with waterfall visualization
- [ ] Genie Space created with all gold tables and metric views
- [ ] Genie Space answers sample certified questions accurately
- [ ] Source-to-gold latency under 5 minutes for streaming path (near real-time target)
- [ ] Source-to-gold latency under 15 minutes for batch file path
- [ ] End-to-end lineage visible in Unity Catalog
