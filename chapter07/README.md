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
| Apache Hudi | 1.2.0 |
| Apache Flink | 1.20 |
| Flink CDC | 3.2.0 |
| Kafka (Confluent) | 7.6.1 |
| MySQL | 8.0 |

## Setup Instructions

1. **Start the environment:**
   ```bash
   cd chapter07
   ./scripts/setup.sh
   ```
   This builds the Flink Docker image (with Hudi + CDC JARs), starts all services, seeds MySQL with 10 sample merchants, and pushes 20 sample transaction events to Kafka.

2. **Run SQL files one at a time** (follow along with the chapter):
   ```bash
   ./scripts/run_section.sh sql/01_create_kafka_source.sql
   ./scripts/run_section.sh sql/02_create_hudi_sink_flink_state.sql
   ./scripts/run_section.sh sql/03_run_ingestion.sql
   ```

3. **Or run everything at once:**
   ```bash
   ./scripts/run_all.sh
   ```

4. **Tear down when done:**
   ```bash
   ./scripts/teardown.sh
   ```

## Tutorial Steps

### Section 7.2: Streaming Event Ingestion
| File | Description |
|------|-------------|
| `sql/01_create_kafka_source.sql` | Kafka source table for payment events |
| `sql/02_create_hudi_sink_flink_state.sql` | Hudi MoR sink with FLINK_STATE index |
| `sql/03_run_ingestion.sql` | INSERT INTO to start the streaming pipeline |
| `sql/04_verify_ingestion.sql` | Verification queries |

### Section 7.3: Scaling with Bucket Index
| File | Description |
|------|-------------|
| `sql/05_switch_to_bucket_index.sql` | Recreate table with BUCKET index (128 buckets) |
| `sql/06_run_ingestion_bucket.sql` | Re-run ingestion with new config |

### Section 7.4: Non-Blocking Concurrency Control
| File | Description |
|------|-------------|
| `sql/07_enable_nbcc.sql` | Add NBCC + StorageBasedLockProvider |

### Section 7.5: Event-Time Ordering
| File | Description |
|------|-------------|
| `sql/08_event_time_ordering.sql` | Switch to EVENT_TIME_ORDERING with record_version |

### Section 7.6: CDC Replication
| File | Description |
|------|-------------|
| `sql/09_create_mysql_cdc_source.sql` | Flink CDC source for MySQL merchants |
| `sql/10_create_hudi_merchants_sink.sql` | Hudi MoR sink with changelog enabled |
| `sql/11_run_cdc_pipeline.sql` | Start CDC replication |
| `sql/12_verify_cdc.sql` | Verification queries |

### Section 7.7: Medallion Pipeline
| File | Description |
|------|-------------|
| `sql/13_create_incremental_sources.sql` | Incremental read tables for both Bronze tables |
| `sql/14_create_silver_sink.sql` | Silver enriched_transactions table (64 buckets) |
| `sql/15_run_silver_pipeline.sql` | Silver pipeline: join transactions with merchants |
| `sql/16_create_gold_table.sql` | Gold daily summary table (CoW, 2 buckets) |
| `sql/17_run_gold_aggregation.sql` | Gold aggregation batch |

### Section 7.8: Production Operations
| File | Description |
|------|-------------|
| `sql/18_partition_ttl.sql` | Partition TTL and cleaner configuration examples |
| `sql/19_insert_overwrite_recovery.sql` | INSERT OVERWRITE for partition recovery |
| `sql/20_hudi_cli_commands.sh` | Hudi CLI commands for timeline debugging |

## Files in This Chapter

```
chapter07/
  README.md                           # This file
  Dockerfile                          # Flink 1.20 + Hudi/CDC JARs
  docker-compose.yml                  # Flink, Kafka, Zookeeper, MySQL
  data/
    merchants_seed.sql                # 10 sample merchants (loaded into MySQL on startup)
    sample_transactions.jsonl         # 20 sample Kafka events
  sql/
    01-20 SQL files                   # One per chapter section (see above)
  scripts/
    setup.sh                         # Start environment, seed data, produce events
    run_section.sh                   # Run a single SQL file through Flink SQL Client
    run_all.sh                       # Run all SQL files in order
    produce_events.sh                # Push sample events to Kafka
    teardown.sh                      # Stop everything, remove volumes
```

## Troubleshooting

- **Flink UI not loading at http://localhost:8081**: Wait 30 seconds after `docker compose up` — the JobManager takes time to initialize.
- **CDC pipeline fails with "Access denied"**: Ensure MySQL started with binlog enabled. The seed script grants REPLICATION permissions to `cdc_reader`.
- **Out of memory**: Flink + Kafka + MySQL need ~6-8 GB. Increase Docker's memory limit.
- **SQL Client errors about missing JARs**: The Dockerfile downloads Hudi, CDC, and Kafka connector JARs at build time. If the build failed, check your internet connection and re-run `docker compose build`.

## Sample Data

**Merchants** (10 rows in MySQL): Covers food_delivery, retail, electronics, grocery, media, and financial categories. Includes one suspended merchant (M006) and one test merchant (M010).

**Transactions** (20 events in Kafka): 10 unique transaction_ids across 8 merchants, with multiple event types (AUTHORIZATION, SETTLEMENT, REFUND, CHARGEBACK) demonstrating upsert behavior. Currencies: USD, GBP, CAD, INR.

## Further Reading

- [Apache Hudi with Flink](https://hudi.apache.org/docs/flink-quick-start-guide)
- [Flink CDC Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-stable/)
- [Hudi Concurrency Control](https://hudi.apache.org/docs/concurrency_control)
- [Hudi CLI Reference](https://hudi.apache.org/docs/cli)
