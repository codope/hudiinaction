# Production-readiness gate: trip_updates

The full checklist behind Chapter 10's "Completing the production-readiness gate". The agent
ends with this list rather than an unconditional "ready"; a reviewer should see every item
answered, with evidence, before approving launch. Items come from the "What you must still
check" notes and open items earlier in the chapter.

## 1. Requirements and correctness

- [ ] Every confirmed fact in the workload profile (Table 10.3) has a named owner.
- [ ] Every assumption that affects correctness has a validation task, with a result:
  - [ ] `updated_at` reflects source ordering under replay.
  - [ ] Corrections preserve the original `trip_date`.
  - [ ] `trip_id` is globally unique, and the source always supplies the correct `trip_date`.
  - [ ] The transformation remains stateless.
- [ ] Five-minute visibility is tested on representative infrastructure: update one trip, then
      read it through the actual dashboard query before compaction runs.

## 2. Capacity and services

- [ ] Peak and catch-up ingestion meet the visibility target. The replay experiment covers high,
      median, and low volume and measures bytes per output file, small-file ratio, and tagging,
      shuffle, and commit duration separately.
- [ ] Record-level index sizing is validated before launch. Afterwards, existing partitions
      need an index rebuild.
  - [ ] The 100 million busiest-day figure is a projection the team stands behind, not the
        busiest day seen so far.
  - [ ] Keys are UUID-shaped, so ~50 bytes per index entry holds.
  - [ ] `min.filegroup.count` == `max.filegroup.count` == 12 in the running job.
- [ ] Compaction keeps up, measured as backlog: oldest pending compaction age, log files per file
      group, and the snapshot merge-time trend.
- [ ] The history settings reached the running job: a time-travel query just inside the 48-hour
      boundary succeeds after cleaning and archival have both run.
- [ ] Incremental readers behave correctly across an archival boundary and keep up with the
      table's uncleaned timeline.
- [ ] The consuming engines exploit column statistics, confirmed with an explain-plan check
      against the production reader.

## 3. Concurrency and discoverability

- [ ] Every writing job is named: the ingestion pipeline, the nightly correction job, backfills,
      cleanup jobs, and standalone services.
- [ ] Every one of those jobs carries the identical concurrency block from
      `config/trip_updates.properties` and has `hudi-aws-bundle` on its classpath.
- [ ] The lock provider is confirmed against the platform standard, and every writing job has
      IAM permissions for the DynamoDB lock table.
- [ ] Catalog registration is verified from the consuming engine (Trino), not from the writer's
      logs.
- [ ] The partition extractor matches the physical path format the key generator writes.
- [ ] The consumer contract is recorded: which consumers read `trip_updates_ro` and which read
      `trip_updates_rt`. Trino reads `_rt` through the hudi-trino connector, because the built-in
      connector reads Merge-on-Read tables in read-optimized mode.

## 4. Operations

- [ ] Alerts cover commits, table services (compaction backlog, cleaning, archival), catalog
      staleness, and lock conflicts (acquisition failures and conflict-retry rate).
- [ ] Active timeline size is monitored from day one.
- [ ] Runbooks define restart, rollback, and re-bootstrap, including the record-index rebuild:
      stop the writers, remove the index with the Hudi CLI's `metadata delete-record-index`, and
      rebuild it in the next write or with HoodieIndexer `--index-types RECORD_INDEX`.
- [ ] Deployment begins with a canary or shadow table.

## 5. Governance

- [ ] The ADR is reviewed by the source owner, the platform owner, and key consumers.
- [ ] The five migration-sensitive decisions (Table 10.5: record key, partitioning, table type,
      record-level index file-group count, ordering field) are explicitly approved.
- [ ] Every override and accepted warning carries a measurable revisit trigger
      (`design/02_scale_and_read_path.yaml` lists the five revisit conditions).
- [ ] The target Hudi version (1.2.1) is recorded, so a future review can detect stale advice.
