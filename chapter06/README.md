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

- **Handling update-heavy workloads**
  - Simulating retry storms (real production issue)

- **Table services in action**
  - Compaction, clustering, metadata table

- **Failure recovery**
  - Savepoints and restore

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

## 🔥 Simulating Retry Storm
./scripts/run_producer.sh retry_storm 5000

## Restore
./scripts/restore_demo_savepoints.sh

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
