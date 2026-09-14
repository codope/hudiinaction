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
- About 100 MB of free space under `/tmp`

No Hudi installation is needed. The scripts pull `hudi-spark3.5-bundle_2.12:1.2.0`.

## Versions

| Component | Version |
|-----------|---------|
| Apache Hudi | 1.2.0 |
| Apache Spark | 3.5 |
| Scala | 2.12 |

Override with `HUDI_VERSION` or `SPARK_BUNDLE` if you are testing another combination.

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

`show_layout.sh` is where the trade in Figure 9.1 becomes concrete. The base files under
`trip_date=2023-*` and `trip_date=2024-*` are skeleton files holding only the five Hudi
metadata columns, so they are far smaller than the source files whose rows they stand for,
and those source files are still the only copy of the data. The `trip_date=2025-*`
partitions were rewritten in full and no longer refer to the source at all.

Delete or move the source directory and query the table again to see the dependency that
*Living with a bootstrapped table* describes. The metadata-only partitions fail; the
full-record ones keep working.

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
rows. Every Hudi option is otherwise identical to the chapter.
