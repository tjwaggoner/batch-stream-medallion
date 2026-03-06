# Batch + Stream Medallion Pipeline

A unified batch and streaming data pipeline built on Databricks using Spark Declarative Pipelines (SDP) with a 4-layer medallion architecture. Demonstrates how a single lakehouse platform can replace a multi-system stack of file ingestion frameworks, message brokers, legacy warehouses, and standalone OLAP engines.

See [PRD.md](PRD.md) for the full specification.

## Medallion Architecture

| Layer | Schema | What It Does | Why It Exists |
|-------|--------|-------------|---------------|
| **Pre-Bronze** | `prebronze` | Append-only raw ingestion from Kafka (streaming) and S3 volume (batch via Auto Loader). Every file and event preserved exactly as received with metadata (`_ingested_at`, `_source_file`, `_source_type`). | **Immutable audit trail.** Partner files in S3 are the source of record, but once processed they may be archived or deleted. Pre-bronze guarantees you can always replay or investigate what was received and when. Essential for reconciliation, debugging data quality issues, and regulatory compliance in fintech. Without this layer, a bad merge in bronze would lose the original data. |
| **Bronze** | `bronze` | Merge/upsert from pre-bronze using AUTO CDC (SCD Type 1). Deduplicates full-snapshot files, merges incremental files on natural keys. Produces one clean, current-state table per source. Streaming events pass through unchanged. | **Clean foundation for downstream joins.** Full-snapshot files (users, balances, linked accounts) arrive daily with every row repeated — merging here prevents silver from having to deduplicate millions of rows on every refresh. Bronze is the "what does the data look like right now?" layer. Silver shouldn't have to answer that question. |
| **Silver** | `silver` | 3NF normalized entity tables. Two table types: (1) **AUTO CDC SCD Type 2** for single-source entities that need history (users, cards), and (2) **Materialized views** for multi-source joins (accounts, transactions, alerts, etc.). Cross-source joins, static-stream joins, data conforming. | **Business-ready entities with unified batch + stream.** This is where the static-stream join happens — merged bronze tables (static) are joined with the live Kafka stream to produce a single `transactions` table from 4 batch payment types + streaming events. Silver answers "what are the business entities, how do they relate, and how have they changed?" |
| **Gold** | `gold` | Denormalized fact tables (materialized views), dimension tables (materialized views reading current state from silver SCD2 via `WHERE __END_AT IS NULL`), aggregations, and Unity Catalog materialized metric views. Optimized for BI, dashboards, Genie, and ad-hoc analytics. | **Consumption-ready analytics.** No joins required for common queries. Dimension tables expose current state from silver's SCD2 tables — full history remains queryable in silver for point-in-time analysis. Materialized metric views enforce governed KPI definitions with pre-computed aggregations. Genie Space enables natural language access for non-technical users. |

### Why Pre-Bronze?

In a standard 3-layer medallion (bronze → silver → gold), bronze serves double duty: raw ingestion *and* dedup/merge. This works when sources are simple, but breaks down when:

1. **Full-snapshot files are large** — Daily files with 10M+ rows mean bronze must either store every snapshot (massive duplication) or merge and lose the raw data. Pre-bronze solves this by storing the raw append, while bronze merges to current state.

2. **Auditability matters** — In fintech, you need to answer "what data did we receive from the partner on March 3rd?" If bronze has already merged, that answer is gone. Pre-bronze preserves the exact file contents with ingestion timestamps.

3. **Backfill and replay** — If a bronze merge goes wrong (bad schema evolution, corrupt file), you can re-derive bronze from pre-bronze without re-ingesting from the source system. The source S3 volume may have already been cleaned up.

4. **Separation of concerns** — Pre-bronze owns "get data in reliably." Bronze owns "make it queryable." Silver owns "make it meaningful." Each layer has one job.

### Why Two Table Types in Silver?

Silver has two kinds of tables — **AUTO CDC (SCD Type 2)** and **materialized views** — because they solve different problems:

**AUTO CDC** works by watching a single source table for changes. When a row changes, it automatically creates a new version with timestamps (`__START_AT`, `__END_AT`) so you can see the full history. This is great for entities like `users` or `cards` where you need to answer "what did this user's profile look like last month?" But AUTO CDC can only watch **one source at a time** — it has no way to detect that a change in `bronze_linked_accounts` should trigger an update in a table that also depends on `bronze_account_balances`.

**Materialized views** solve the multi-source problem. Instead of tracking individual row changes, a materialized view re-executes its SQL query on each pipeline refresh. If `bronze_account_balances` changed, or `bronze_linked_accounts` changed, or both — the materialized view simply recomputes the joined result and you get the correct current state. No change-detection wiring needed.

| Scenario | Use | Why |
|----------|-----|-----|
| Single bronze source, need history | AUTO CDC (SCD Type 2) | Efficient incremental processing, automatic version tracking |
| Multiple bronze sources joined | Materialized View | Handles changes from any source automatically on refresh |
| Single bronze source, no history needed | Either works | Materialized view is simpler; AUTO CDC SCD Type 1 is more incremental |

**What about history on joined entities?** For point-in-time analysis on multi-source tables (e.g., "what did this account look like on March 1st?"), query the individual SCD2 tables directly (`silver_users`, `silver_cards`) and join them with bronze tables at the desired point in time. The materialized views always reflect current state.

## Tech Stack

- **Spark Declarative Pipelines (SDP)** — Pipeline framework (SQL)
- **Auto Loader** — Incremental file ingestion via `read_files()`
- **Kafka** — Streaming ingestion via `read_kafka()`
- **AUTO CDC** — Merge/upsert and SCD tracking
- **Unity Catalog** — Governance, lineage, materialized metric views
- **Databricks Asset Bundles** — Deployment and environment management
- **Genie Space** — Natural language query interface for gold layer
- **Serverless compute** — No cluster management
