# Chapter 7: Building a Streaming and CDC Lakehouse with Apache Hudi and Flink

This chapter builds a complete streaming and CDC data platform for a fintech company (NovaPay) using Apache Flink and Apache Hudi. Every pipeline is defined in Flink SQL and runs against a local Docker environment with Flink, Kafka, and MySQL.

## What You'll Learn

- Stream high-throughput events from Kafka into a Hudi MoR table using Flink SQL
- Replicate a MySQL database into the lakehouse in real time using Flink CDC
- Scale writes with bucket index and partition-level bucketing
- Run concurrent pipelines safely with non-blocking concurrency control (NBCC)
- Handle out-of-order events with event-time merge semantics
- Build a Bronze-Silver-Gold medallion architecture with incremental reads
- Recover from data corruption using INSERT OVERWRITE and the Hudi CLI

## Prerequisites

- Docker and Docker Compose (v2)
- ~8 GB free RAM (Flink + Kafka + MySQL)
- No local Flink, Spark, or Hudi installation required — everything runs in Docker

## Versions

| Component | Version |
|-----------|---------|
| Apache Hudi | 1.2.0 (Flink 1.20 bundle) |
| Apache Flink | 1.20 |
| Flink CDC (MySQL SQL connector) | 3.2.0, with MySQL JDBC driver 8.0.27 |
| Flink Kafka connector | 3.3.0-1.20 |
| Hadoop client (needed by the Hudi Flink bundle) | 3.3.6 |
| Apache Kafka | 3.6 (Confluent Platform 7.6.1 images) |
| MySQL | 8.0 |

## Setup Instructions

1. **Start the environment:**
   ```bash
   cd chapter07
   ./scripts/setup.sh
   ```
   This builds the Flink image, starts Flink, Kafka and MySQL, seeds MySQL with 10 merchants, and
   produces 22 transaction events to `payments.transactions` and one corrected event to
   `payments.transactions.replay`.

2. **Run the whole chapter:**
   ```bash
   ./scripts/run_all.sh
   ```
   This runs the chapter in eight stages (below). Each stage starts its streaming jobs, waits for a
   few checkpoints (Flink commits to Hudi on each 30-second checkpoint), runs a verification query,
   and cancels the jobs that the next stage replaces. It takes about 12 minutes.

3. **Or run one file at a time:**
   ```bash
   ./scripts/run_section.sh sql/03_run_ingestion.sql detached   # streaming INSERT
   ./scripts/run_section.sh sql/04_verify_ingestion.sql         # batch query
   ```
   Every file runs in its own Flink SQL Client session, and Flink's default catalog lives in memory.
   So a file that uses tables declared elsewhere names those files on a `-- requires:` line, and the
   scripts prepend them. Streaming INSERT files must be run `detached`; stop them from the Flink UI
   (http://localhost:8081) or with `./scripts/teardown.sh`.

4. **Tear down when done:**
   ```bash
   ./scripts/teardown.sh
   ```

`write.tasks` is 2 in every table so the demo fits one TaskManager; the chapter uses NovaPay's
production values (4 for bronze, 64 for silver).

## Stages and Expected Output

All sample transactions were created on 15 June 2024, so every table has one partition,
`2024-06-15`. TXN-0011 is authorized at 23:58 and settled at 00:04 the next day; because `dt` comes
from `created_ts`, it stays a single row in `2024-06-15`.

| Stage | Chapter section | Files | Expected output |
|---|---|---|---|
| 1 | Streaming event ingestion | 01–04 | 11 transactions: authorized 2, chargeback 1, refunded 2, settled 6 |
| 2 | Scaling writes with bucket index | 05, 06, 06b | `rows_total = 11`, `transactions = 11`; TXN-0011 settled in `2024-06-15` |
| 3 | Running concurrent pipelines with NBCC | 07, 07b, 07c | Both writers commit; 11 rows. TXN-0002 shows M002 or M004 depending on which writer committed last (commit-time ordering) |
| 4 | Handling out-of-order events | 08, 08b, 08c | TXN-0002 = M004, `record_version` 3, whatever the commit order |
| 5 | Replicating MySQL with Flink CDC | 09–12 | 10 merchants: electronics 1, financial 1, food_delivery 2, grocery 2, media 1, retail 3 |
| 6 | Building the silver layer | 13–15b | 11 enriched rows; TXN-0002 enriched with TechGadgets Plus (electronics) |
| 7 | Building the gold layer | 16, 17 | 6 rows keyed by (dt, category, currency); grocery appears twice, CAD 67.30 and GBP 55.00 |
| 8 | Recovering a partition with INSERT OVERWRITE | 19 | Staging 11 rows, 2324.72 total; partition `2024-06-15` has 11 rows after the overwrite |

`18_partition_ttl.sql` and `20_hudi_cli_commands.sh` are reference only (configuration and CLI
commands from the chapter); `run_all.sh` does not execute them.

## Troubleshooting

- **Flink UI not loading at http://localhost:8081**: Wait 30 seconds after `docker compose up` — the JobManager takes time to initialize.
- **CDC pipeline fails with "Access denied"**: Ensure MySQL started with binlog enabled. The seed script grants REPLICATION permissions to `cdc_reader`.
- **Out of memory**: Flink + Kafka + MySQL need ~6-8 GB. Increase Docker's memory limit.
- **SQL Client errors about missing JARs**: The Dockerfile downloads the Hudi, Flink CDC, MySQL JDBC, Kafka connector and Hadoop client JARs at build time. If the build failed, check your internet connection and re-run `docker compose build`.
- **TLS errors pulling images or downloading JARs behind a corporate proxy**: the Docker VM and the image build must trust your proxy's root CA.

## Sample Data

**Merchants** (10 rows in MySQL): Covers food_delivery, retail, electronics, grocery, media, and financial categories. Includes one suspended merchant (M006) and one test merchant (M010).

**Transactions** (22 events in Kafka): 11 unique transaction_ids across 8 merchants, each event carrying `created_ts` (the transaction's creation time) and `record_version`, with multiple event types (AUTHORIZATION, SETTLEMENT, REFUND, CHARGEBACK) demonstrating upsert behavior. Currencies: USD, GBP, CAD, INR.

## Further Reading

- [Apache Hudi with Flink](https://hudi.apache.org/docs/flink-quick-start-guide)
- [Flink CDC Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-stable/)
- [Hudi Concurrency Control](https://hudi.apache.org/docs/concurrency_control)
- [Hudi CLI Reference](https://hudi.apache.org/docs/cli)
