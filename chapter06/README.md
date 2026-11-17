# 📘 Chapter 6 — Building a Real-Time Marketplace Payout Lakehouse with Apache Hudi

This chapter walks through building a **production-grade lakehouse pipeline** using Apache Hudi.

Unlike earlier chapters focused on individual features, this chapter brings everything together into a **real-world system**:

- Streaming ingestion
- Incremental processing
- Update-heavy workloads
- Failure recovery
- Governance (GDPR, audit, time travel)

---

# 🚀 What You Will Learn

In this chapter, you will build and understand:

- **Streaming ingestion with Hudi**
  - Kafka → Hudi raw table (CoW)

- **Layered lakehouse design**
  - Raw (immutable ledger)
  - Silver (aggregated business view)

- **Incremental pipelines**
  - Raw → Silver using HoodieStreamer + HoodieIncrSource

- **Late and out-of-order events**
  - Versioned order line snapshots, resolved by the ordering field

- **Table services in action**
  - Compaction, clustering, metadata table

- **Failure recovery**
  - Recovering from polluted upstream data with savepoints and restore

- **Governance workflows**
  - GDPR deletes
  - Time travel for auditability

---

# 🧱 Architecture Overview

Kafka → Raw (Hudi CoW) → Silver (Hudi MoR)

---

# 🛠️ Prerequisites

- Java 11+
- Scala 2.12
- sbt
- Apache Spark 3.5.x
- Apache Hudi 1.2.1 (pulled by the scripts via `--packages`)
- Docker + Docker Compose

---

# 🚀 Quick Start (End-to-End Demo)

## 1. Start Kafka
docker compose up -d

## 2. Build the project
sbt clean package

## 3. Produce normal events
./scripts/run_producer.sh normal 2000

Produce events before starting raw ingestion. The first send creates the `payments.events` topic,
and the raw ingestion job fails with `UnknownTopicOrPartitionException` if the topic does not
exist yet.

## 4. Start raw ingestion (in its own terminal)
./scripts/run_raw_ingestion.sh

The job keeps running and commits a micro-batch every 5 minutes. The first commit is written
straight away; wait for it before starting the silver job:

ls /tmp/hudi/raw/payments/.hoodie/timeline/*.commit

## 5. Start the silver pipeline (in its own terminal)
./scripts/run_silver_hoodiestreamer.sh

Both jobs run until you stop them, so send further events from a third terminal.

---

# 🐍 Python Version (no sbt)

Every Spark job in this chapter also has a PySpark version in `pyspark/`, and the event producer
has a plain Python version, so the whole demo runs without Scala or sbt. You need Python 3.8 or
later and the producer's Kafka client:

pip install -r pyspark/requirements.txt

Run the scripts from the `chapter06` directory, as with the Scala ones. Each Scala script has a
Python counterpart:

| Step | Scala (needs `sbt clean package`) | Python |
|---|---|---|
| Produce events | `run_producer.sh` | `run_producer_python.sh` |
| Raw ingestion | `run_raw_ingestion.sh` | `run_raw_ingestion_pyspark.sh` |
| Silver HoodieStreamer | `run_silver_hoodiestreamer.sh` | `run_silver_hoodiestreamer_sql.sh` |
| Savepoints | `create_demo_savepoints.sh`, `restore_demo_savepoints.sh` | `create_demo_savepoints_pyspark.sh`, `restore_demo_savepoints_pyspark.sh` |
| GDPR delete | `run_gdpr_delete.sh` | `run_gdpr_delete_pyspark.sh` |
| Time travel | `run_time_travel.sh` | `run_time_travel_pyspark.sh` |

The arguments are the same, so every command in the sections below works with the Python script
in its place.

The silver job is the one that changes approach. `VendorPayoutTransformer` is a Scala class
that HoodieStreamer loads by name, so it cannot be written in Python. Its logic is a single
projection, so `run_silver_hoodiestreamer_sql.sh` uses Hudi's built-in `SqlQueryBasedTransformer`
with the same query, in `conf/silver-hoodiestreamer-sql.properties`, and runs Hudi's utilities
bundle (downloaded once into `/tmp/hudi_bundles`) as the application jar. The chapter describes
the custom class; the SQL version writes the same silver rows (only `last_updated_ts`, the
time of the run, differs).

---


---

# 🛟 Savepoint-Based Recovery

## Savepoint
./scripts/create_demo_savepoints.sh

## 🔥 Simulating polluted upstream data
./scripts/run_producer.sh polluted 30

A buggy payment-service release sends one more event for order lines 1–30. The events carry
valid event IDs and higher `line_version` values, but `gross_to_date` is ten times too high for
every third line. Raw commits them and HoodieStreamer propagates them, so silver now reports
inflated gross and net payouts.

## Restore
Stop both ingestion pipelines (raw ingestion and HoodieStreamer), then:

./scripts/restore_demo_savepoints.sh

## Replay corrected events
Restart both pipelines, then send the same events with correct values:

./scripts/run_producer.sh polluted 30 fixed

---

# 🔐 GDPR Delete Workflow
./scripts/run_gdpr_delete.sh V-101

---

# ⏳ Time Travel
./scripts/run_time_travel.sh silver <INSTANT_TIME> V-101

---

# 📁 Project Structure

chapter06/
├── conf/
├── project/        (sbt version)
├── pyspark/
├── scripts/
├── schemas/
├── src/
├── docker-compose.yml
└── README.md

---

# 🎯 Expected Outcomes

- Working streaming lakehouse pipeline
- Recovery from corruption using savepoints
- GDPR delete handling
- Time travel audit capability

---

# ⚠️ Troubleshooting

Kafka not running:
docker compose up -d

Raw ingestion fails with `UnknownTopicOrPartitionException`:
the `payments.events` topic does not exist yet. Run the producer first, then restart raw ingestion.

sbt missing:
brew install sbt

---

# 🔥 Final Note

This is a **production-style reference pipeline**, not just a demo.
