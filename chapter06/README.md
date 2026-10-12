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

## 3. Start ingestion pipelines
./scripts/run_raw_ingestion.sh
./scripts/run_silver_hoodiestreamer.sh

## 4. Produce normal events
./scripts/run_producer.sh normal 2000

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

sbt missing:
brew install sbt

---

# 🔥 Final Note

This is a **production-style reference pipeline**, not just a demo.
