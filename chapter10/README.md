# Chapter 10: Designing Production Lakehouse Pipelines

The design package for the chapter's `trip_updates` pipeline: the Hudi Architect checkpoints, the
grouped configuration bundle, the submission command, and the full production-readiness gate.
The chapter is design-oriented, so these files are artifacts to review and adapt, not a demo that
runs on a laptop: the pipeline needs Kafka with a schema registry, S3, DynamoDB, and AWS Glue.

| File | What it is | Chapter section |
|---|---|---|
| `design/01_architecture.yaml` | Architecture decisions, assumptions, and open items | *Deriving the ingestion and table architecture* |
| `design/02_scale_and_read_path.yaml` | Record-level index sizing, read path, deferred decisions, revisit conditions | *Designing for scale and query performance* |
| `design/03_services_concurrency_catalog.yaml` | Table services, archival arithmetic, concurrency, catalog sync | *Deriving table services, retention, and concurrency* |
| `config/trip_updates.properties` | The grouped configuration bundle passed as `--props` | *Grouping configuration by decision* |
| `scripts/submit_trip_updates.sh` | The HoodieStreamer submission command | *Generating the submission command* |
| `production_readiness_gate.md` | The full gate behind the chapter's five headings | *Completing the production-readiness gate* |

## Using the bundle

1. Replace every `<placeholder>` in `config/trip_updates.properties`: lock table and region, Glue
   database, Kafka brokers and topic, and schema registry URL.
2. Not on AWS: replace the lock provider lines with your platform's standard, and drop
   `hudi-aws-bundle` and the Glue sync flags if you register the table elsewhere.
3. Set the environment variables listed at the top of `scripts/submit_trip_updates.sh`, and pass
   your platform's master, deploy mode, memory, and executor flags as arguments.
4. Every other job that writes the table, including the nightly correction job, needs the same
   concurrency block and `hudi-aws-bundle`.
5. Work through `production_readiness_gate.md` before launch.

All `hoodie.*` keys and class names were checked against the Apache Hudi 1.2.1 source.
