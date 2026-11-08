# Chapter 5: Managing Tables Using Table Services

Runnable versions of the chapter's examples: file sizing, compaction, clustering, cleaning,
rollback, savepoint and restore, and asynchronous indexing.

| File | What it covers | Chapter section |
|---|---|---|
| `spark/01_file_sizing.scala` | Write-time file sizing on a CoW table, compared with `bulk_insert` | *Managing file sizing and small files* |
| `spark/02_inline_compaction.scala` | Inline compaction with the `NUM_OR_TIME` trigger | *Configuring compaction with Spark datasource writer* |
| `spark/03_schedule_compaction.scala` | Writer schedules compaction, a separate step executes it | *Scheduling compaction inline and executing asynchronously* |
| `spark/04_streaming_async_compaction.scala` | Structured Streaming into MoR with async compaction | *Configuring async compaction with Spark Structured Streaming* |
| `spark/05_inline_clustering.scala` | Inline clustering with size-based grouping and a sort | *Configuring Inline Clustering* |
| `spark/06_cleaning.scala` | Inline, async, and time-based cleaner policies | *Configuring Cleaner policies* |
| `spark/07_recovery.scala` | Rollback, savepoint, and restore through Spark SQL procedures | *Recovering from failures and disasters* |
| `cli/hudi_cli_recovery.md` | The same recovery steps with the Hudi CLI, as the chapter shows them | *Recovering from failures and disasters* |
| `scripts/run_streamer_compaction.sh` + `streamer/compaction.properties` | HoodieStreamer continuous mode with async compaction | *Configuring compaction with HoodieStreamer* |
| `scripts/run_async_indexing.sh` + `streamer/write.properties`, `streamer/indexer.properties` | HoodieIndexer builds a record index while HoodieStreamer keeps writing | *Asynchronous indexing* |

## Requirements

- Java 11 or 17
- Apache Spark 3.5.x, pre-built for Hadoop 3 (`spark-shell` on the PATH; `SPARK_HOME` set for
  the HoodieStreamer scripts)
- Apache Hudi 1.2.1 (pulled by `--packages` or downloaded by the scripts)
- macOS or Linux. On Windows, run everything inside WSL2.

The HoodieStreamer examples reuse Chapter 3's NYC taxi sample (`chapter03/hudi_streamer.tar.gz`),
extracted under `/tmp/hudiinaction/chapter05`.

## Run

```bash
cd chapter05/scripts
./run_spark_examples.sh                       # all seven spark-shell examples, in chapter order
./run_spark_examples.sh 05_inline_clustering  # or just one
./run_streamer_compaction.sh
./run_async_indexing.sh
```

Each spark-shell example deletes and recreates its own tables under `/tmp/hudiinaction/chapter05`,
prints what changed on the timeline or in the file layout, and exits. To run one by hand:

```bash
./spark_shell_with_hudi.sh --driver-memory 2g -i ../spark/02_inline_compaction.scala
```

## Differences from the chapter's snippets

- **Hudi version.** The chapter's `spark-submit` commands use 1.0.2 bundles; these examples use 1.2.1.
- **Ordering field.** The examples set `hoodie.table.ordering.fields`. The chapter's
  `hoodie.datasource.write.precombine.field` is deprecated in 1.x.
- **Record index key.** `hoodie.metadata.record.index.enable` is now an alias of
  `hoodie.metadata.global.record.level.index.enable`; the properties files use the current key.
- **Async indexing writer.** The writer also sets `hoodie.metadata.index.async=true` and enables the
  record index from the start. With both set, the writer leaves building the index to HoodieIndexer
  and keeps it up to date afterwards, which is the requirement the chapter's note describes.
- **HoodieIndexer flags.** `--parallelism` is required in 1.2.1.
- **Record key.** The chapter's `trip_id`, `vendor_id`, and `updated_time` fields are not in the NYC
  sample, so the indexing example uses Chapter 3's six-column key and `date_col` partitioning.
- **Lock provider.** Both streamer examples use `FileSystemBasedLockProvider`, which is safe on local
  disk and HDFS only. The chapter's ZooKeeper settings are kept, commented out, in
  `streamer/write.properties`.
- **Clustering input.** `05_inline_clustering.scala` sets `hoodie.parquet.small.file.limit=0` so that
  each insert creates new small files for clustering to merge. Leave that setting out in real tables.
