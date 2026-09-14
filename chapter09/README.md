# Chapter 9: Advanced Lakehouse Capabilities

Chapter 9 is a survey of the capabilities a production lakehouse platform needs: indexing
scope, record merge modes, concurrency control, bootstrap, schema evolution, and reading the
same data from other engines. Most of it is explained against configuration you have already
met in earlier chapters, so the chapter carries a single runnable example, and that is what
this directory holds.

The example is the bootstrap from *Running a bootstrap*: adopting a directory of existing
Parquet as a Hudi table without rewriting it, using one mode for cold history and another for
recent partitions.

## What this shows

- A bootstrap that references existing files in place rather than copying them
- `BootstrapRegexModeSelector` applying `METADATA_ONLY` to partitions matching
  `trip_date=202[0-4]-.*` and `FULL_RECORD` to everything newer
- The skeleton files, the bootstrap index under `.hoodie/.aux/.bootstrap/`, and the source
  files the metadata-only partitions still depend on, which is Figure 9.1 as real files
- The Hudi metadata columns being available on rows whose data never moved

## Prerequisites

Either a local Spark, or Docker.

- Apache Spark 3.5 with `spark-shell` on your `PATH`, and a JDK Spark 3.5 supports
- Network access on first run, since the Hudi bundle is pulled with `--packages`
- About 5 MB of free space under `/tmp`

No Hudi installation is needed. The scripts pull `hudi-spark3.5-bundle_2.12:1.2.0`.

## Versions

| Component | Version |
|-----------|---------|
| Apache Hudi | 1.2.0 |
| Apache Spark | 3.5 |
| Scala | 2.12 |

Override with `HUDI_VERSION` or `SPARK_BUNDLE` if you are testing another combination. The output
below was produced on Spark 3.5.6 with Amazon Corretto 17 on macOS (arm64).

## Layout

```
chapter09/
  docker-compose.yml           optional Spark container
  scripts/
    spark_shell_with_hudi.sh   launches spark-shell with the Hudi bundle and settings
    generate_source_data.sh    writes the pre-existing Parquet dataset
    run_bootstrap.sh           runs the bootstrap
    verify_bootstrap.sh        queries the bootstrapped table
    show_layout.sh             prints the on-disk layout, source against table
    run_all.sh                 the four above, in order
    teardown.sh                removes the working directory
  spark/
    generate_source_data.scala
    bootstrap_trips.scala
    verify_bootstrap.scala
```

## Running it

With Spark on your machine:

```bash
cd chapter09
./scripts/run_all.sh
```

Or step by step, which is easier to follow alongside the chapter:

```bash
./scripts/generate_source_data.sh   # eight trips across five partitions, plain Parquet
./scripts/run_bootstrap.sh          # the chapter's bootstrap job
./scripts/verify_bootstrap.sh       # query the table, read its properties
./scripts/show_layout.sh            # what each mode left on disk
```

With Docker instead:

```bash
cd chapter09
docker compose up -d
docker compose exec spark bash
export PATH="$PATH:/opt/spark/bin"
./scripts/run_all.sh
```

Paths default to `/tmp/hudiinaction/chapter09`. Set `SOURCE_PATH` and `TABLE_PATH` to put
them somewhere else.

## What to look for

The difference between the two modes is in what each base file contains, which
`verify_bootstrap.sh` prints by reading the files directly, without going through Hudi:

```
METADATA_ONLY base file: _hoodie_commit_time, _hoodie_commit_seqno, _hoodie_record_key,
                         _hoodie_partition_path, _hoodie_file_name
FULL_RECORD base file:   _hoodie_commit_time, _hoodie_commit_seqno, _hoodie_record_key,
                         _hoodie_partition_path, _hoodie_file_name, trip_id, rider_id,
                         city, fare, updated_at, trip_date
```

The skeleton holds the five Hudi metadata columns and nothing else. Its data columns are
still in the source Parquet, and the query that returned all eight trips stitched the two
halves together. The full-record partitions carry everything and no longer refer to the
source.

Two further things the run shows. The metadata-only partitions commit at instant
`00000000000001` and the full-record ones at `00000000000002`, so a mixed-mode bootstrap
lands as two commits rather than one. And `hoodie.properties` records
`hoodie.bootstrap.base.path` along with `hoodie.bootstrap.index.type=HFILE`, which is the
dependency *Living with a bootstrapped table* warns about: delete or move the source
directory and the metadata-only partitions stop reading, while the full-record ones keep
working.

The run also demonstrates the derivation rule from *Deriving the merge mode from the ordering
field*. Nothing in the job sets a merge mode, yet the `hoodie.properties` it writes contains
`hoodie.record.merge.mode=EVENT_TIME_ORDERING`, because an ordering field is configured. Drop
`hoodie.table.ordering.fields` and Hudi would resolve `COMMIT_TIME_ORDERING` instead.

File sizes are not the thing to look at here. Both kinds of base file come out around 424 KB
in this run, because fixed per-file overhead dwarfs eight rows. What metadata-only saves is
the rewrite itself, and that shows at real volumes rather than on a toy dataset.

Two warnings during the query are expected and harmless:

```
WARN HFileBootstrapIndexReader: No value found for partition key (trip_date=2025-02-14)
WARN HFileBootstrapIndexReader: No value found for partition key (trip_date=2025-06-30)
```

Those are the full-record partitions, which have no bootstrap index entry because they do
not reference anything.

## Two constraints the example ran into

Both are real at Hudi 1.2.0, and both are set in `bootstrap_trips.scala`:

- **A bootstrap cannot build the column-stats index.**
  `SparkBootstrapCommitActionExecutor.updateColumnsToIndexForColumnStats` throws
  `HoodieNotSupportedException: col stats is not supported with bootstrap operation`, and
  `hoodie.metadata.index.column.stats.enable` defaults to true on Spark, so the job needs it
  set to false.
- **A full-record bootstrap needs an ordering field.**
  `SparkFullBootstrapDataProviderBase` reads the ordering field for every source row and
  passes it to `HoodieAvroUtils.getNestedFieldValAsString` without a null check, so a job
  with no ordering field fails with `NullPointerException: Cannot invoke "String.split"
  because "fieldName" is null`. The metadata-only half succeeds first, which makes this look
  like a partial failure. The script sets `hoodie.table.ordering.fields` to `updated_at`.

`hoodie.bootstrap.parallelism` is also lowered from its default of 1500, which otherwise
schedules 1500 tasks for eight rows.

## Cleaning up

```bash
./scripts/teardown.sh          # removes /tmp/hudiinaction/chapter09
docker compose down -v         # if you used the container
```

## Differences from the chapter's snippet

The chapter prints the same job with `s3://` paths, since that is where the problem lives in
practice. These scripts use local directories so the example runs on a laptop. They also
write from `spark.emptyDataFrame` rather than a populated DataFrame: a bootstrap reads its
data from `hoodie.bootstrap.base.path`, so the DataFrame passed to the writer carries no
rows.

Three options are set here that the chapter's snippet does not print, each explained in a
comment in `bootstrap_trips.scala`: `hoodie.metadata.index.column.stats.enable=false` and
`hoodie.table.ordering.fields=updated_at`, which the run requires for the reasons above, and
`hoodie.bootstrap.parallelism=4`, which just keeps a laptop run quick. Every other Hudi
option matches the chapter.
