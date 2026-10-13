# Chapter 4: Querying Hudi tables

Companion code for the chapter's examples, on a sample of the TPC-DS `store_sales` table
(2,880,404 rows, `chapter04/input_data`).

| File | What it covers |
|---|---|
| `hudi_querying_tutorial.scala` | Create the Hudi table; query it with the Spark DataSource API and Structured Streaming |
| `spark_sql_queries.sql` | The same table from `spark-sql`: snapshot query, `CALL show_commits`, `hudi_table_changes` |
| `query_types.scala` | Snapshot, read-optimized, time travel, incremental (`latest_state`), and CDC queries |
| `metasync/` | Hive Metastore, AWS Glue, BigQuery, and DataHub sync |
| `ai_ml/` | Reading without Spark: hudi-rs (Python), DataFusion (Rust), Ray Data, Daft |

## Requirements

- Java 11
- Apache Spark 3.5.x (tested with 3.5.6, pre-built for Hadoop 3)
- Apache Hudi 1.2.1 (pulled by `--packages`)
- macOS or Linux. On Windows, run everything inside WSL2.

## Record key

No single `store_sales` column is unique: `ss_item_sk` has only 18,000 distinct values. The
table's primary key is `(ss_item_sk, ss_ticket_number)`, so every example uses both fields as the
Hudi record key. Hudi stores the composite key in `_hoodie_record_key` as
`ss_item_sk:406,ss_ticket_number:188525`.

## Setup

```bash
cp -r chapter04/input_data /tmp/input_data
rm -rf /tmp/hudi_demo_tbl /tmp/hudi/demo /tmp/hudi_streaming_hudi   # start clean
```

Start spark-shell:

```bash
spark-shell --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1 \
  --driver-memory 4g --executor-memory 4g \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```

`spark-sql` takes the same options.

## Run

In spark-shell, run the files with `:paste` (paste mode is needed because continuation lines
start with a dot):

```
scala> :paste /path/to/hudiinaction/chapter04/hudi_querying_tutorial.scala
scala> query.stop()        // after the first micro-batch has printed
scala> :paste /path/to/hudiinaction/chapter04/query_types.scala
```

`query_types.scala` uses `df` and `writeOpts` from the tutorial, so run both in the same session.
For Spark SQL:

```bash
spark-sql <same options as above> -f chapter04/spark_sql_queries.sql
```

## Expected results

From a run on Spark 3.5.6, Java 11, Hudi 1.2.1, starting from empty paths:

| Step | Result |
|---|---|
| Table after `bulk_insert` | 2,880,404 rows, table version 9, `column_stats` and `files` metadata partitions |
| `count(*) where ss_store_sk = 7` (DataSource and Spark SQL) | 458,194 |
| Structured Streaming, batch 0 | `Total record count 2880404` |
| Time travel `as.of.instant` = load commit | 2,880,404 rows, pre-upsert values |
| Upsert of 50 random rows | `total_update_records_written` = 50 |
| Incremental read, begin = load completion time | 50 rows |
| `hudi_table_changes(..., 'latest_state', <load completion time>)` | 50 rows |
| CDC over the whole timeline (`'earliest'`) | `i` 2,880,404, `u` 30, `d` 5 |
| CDC DataFrame read, begin = load completion time | `u` 30, `d` 5 |

Two Hudi 1.x details the examples rely on:

- `hoodie.datasource.read.begin.instanttime` is a **completion time** and is exclusive. Use the
  `state_transition_time` column of `CALL show_commits`, not `_hoodie_commit_time`. Starting
  from a commit's `_hoodie_commit_time` re-reads that commit.
- An upsert writes whole rows. A DataFrame with only some of the table's columns fails with
  `MissingSchemaFieldException`.

Known issues in Hudi 1.2.1, worked around in `query_types.scala`:

- In CDC `before`/`after` JSON images, DECIMAL values are written as `{"zero":false}`. The CDC
  example changes the INT column `ss_quantity` for that reason.
- The DataFrame CDC reader returns no rows when the begin time is `earliest`.
  `hudi_table_changes(..., 'cdc', 'earliest')` works.

## Catalog sync (`metasync/`)

| Script | Bundle(s) | Status |
|---|---|---|
| `01_hms_sync.scala` | `hudi-spark3.5-bundle_2.12` | Run: write-time sync and standalone `HiveSyncTool` against a local Hive 2.3.9 metastore; both tables read back 2,880,404 rows |
| `02_glue_sync.scala` | + `hudi-aws-bundle` | Keys checked against Hudi 1.2.1 source; not run (needs AWS) |
| `03_bigquery_sync.scala` | + `hudi-gcp-bundle` | Keys checked against Hudi 1.2.1 source; not run (needs GCP) |
| `04_datahub_sync.scala` | + `hudi-datahub-sync-bundle` | Keys checked against Hudi 1.2.1 source; not run (needs DataHub) |

Athena reads Hudi tables written by Hudi 0.14 and 0.15 (table version 6). `02_glue_sync.scala`
writes table version 6 for that reason.

Apache XTable is covered in Chapter 9. Its latest release (0.4.0-incubating) is built on Hudi
0.14, so it reads table-version-6 tables only.

## Reading without Spark (`ai_ml/`)

hudi-rs 0.5.0 (Python package `hudi`) reads Hudi 1.x tables (table versions 8 and 9):

```bash
pip install -r chapter04/ai_ml/requirements.txt
python chapter04/ai_ml/ai_ml_engines.py <commit_time from CALL show_commits>
# snapshot rows: 2880404, and 2880404 rows as of the load commit
```

The Rust example queries the table through Apache DataFusion:

```bash
cd chapter04/ai_ml/rust_datafusion && cargo run --release
```

### Ray Data and Daft

Neither engine reads this chapter's table (written by Hudi 1.2.1, table version 9) yet. The
examples run on small copies they can read (checked October 2026).

**Ray Data** (`ray_data_example.py`, `pip install -r chapter04/ai_ml/requirements-ray.txt`):
- Ray 2.59's `read_hudi` is an alpha API that depends on hudi-rs 0.4. That version reads table
  version 6 only, and reads Merge-on-Read tables in read-optimized mode.
- hudi-rs 0.4 also finds no files in a non-partitioned table written by Hudi 1.2.1.
- `make_ray_table.scala` therefore writes a 10,000-row copy with table version 6, partitioned by
  `ss_store_sk`, and upserts 20 rows.
- Result: the store-7 snapshot read returns 5,113 rows; the incremental read between the two
  commits returns the 20 upserted rows.

```
scala> :paste chapter04/ai_ml/make_ray_table.scala     # prints the two commit times
$ python chapter04/ai_ml/ray_data_example.py <first commit time> <second commit time>
```

**Daft** (`daft_example.py`, `pip install -r chapter04/ai_ml/requirements-daft.txt`):
- Daft 0.7.26 reads Copy-on-Write tables written by Hudi 0.12 to 0.15.
- It fails on any table written by Hudi 1.x, even at table version 6, because it requires the
  `hoodie.table.keygenerator.class` property, which Hudi 1.x does not write.
- `make_daft_table_hudi015.scala` writes a small table with the Hudi 0.15.0 Spark bundle
  instead.

## Further reading

- [SQL queries on different engines](https://hudi.apache.org/docs/sql_queries)
- [Hive Metastore sync](https://hudi.apache.org/docs/syncing_metastore/)
- [AWS Glue Data Catalog sync](https://hudi.apache.org/docs/syncing_aws_glue_data_catalog/)
- [Google BigQuery sync](https://hudi.apache.org/docs/gcp_bigquery/)
- [DataHub sync](https://hudi.apache.org/docs/syncing_datahub/)
- [hudi-rs](https://github.com/apache/hudi-rs)
