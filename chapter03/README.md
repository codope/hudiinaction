# Chapter 3: Apache Hudi ingestion patterns and write workflows

Companion code for the chapter's two hands-on examples:

1. **Write operations** (`hudi_write_operation_tutorial.scala`): bulk_insert with sort modes,
   insert, upsert, delete, insert_overwrite_table, insert_overwrite, and delete_partition on the
   NYC Taxi sample from Chapter 2.
2. **HoodieStreamer** (`run_hudi_streamer.sh` + `hudi_streamer.tar.gz`): continuous ingestion of
   Parquet files into a Merge-on-Read table with asynchronous compaction.

## Requirements

- Java 11
- Apache Spark 3.5.x (tested with 3.5.6, pre-built for Hadoop 3)
- Apache Hudi 1.2.0 (bundles are pulled by `--packages` or downloaded by the streamer script)
- macOS or Linux. On Windows, run everything inside WSL2 and keep the data under the WSL2 file
  system (for example `/tmp`), not under `/mnt/c`.

## 1. Write operations tutorial

### Prepare the data

The tutorial uses the 1,000,660-row NYC Taxi sample from Chapter 2:

```bash
gunzip -k chapter02/trips_0.gz      # creates chapter02/trips_0
```

Edit `inputPath` at the top of `hudi_write_operation_tutorial.scala` to point at that file. The
table is written to `/tmp/trips_table`, which must start empty. To start over at any point:

```bash
rm -rf /tmp/trips_table
```

### Start spark-shell

```bash
spark-shell --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \
  --driver-memory 4g \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'
```

### Run the tutorial

Run the whole file:

```
scala> :paste /path/to/hudiinaction/chapter03/hudi_write_operation_tutorial.scala
```

Or type `:paste`, paste one section, and press Ctrl+D. Paste mode is required because the
multi-line snippets begin continuation lines with a dot, which the plain spark-shell prompt
would evaluate one line at a time.

### Expected results

Every write is followed by a row count per partition (`vendor_id`). From a run on Spark 3.5.6,
Java 11 and Hudi 1.2.0, starting from an empty table:

| After | vendor_id=1 | vendor_id=2 |
|---|---|---|
| bulk_insert (any sort mode) | 468,855 | 531,805 |
| insert of 10,000 new trips | 473,636 | 537,024 |
| upsert of 1,000 updated trips | 473,636 | 537,024 |
| delete of the 8 trips with rate_code_id = 6 | 473,628 | 537,024 |
| insert_overwrite_table with the full input | 468,855 | 531,805 |
| insert_overwrite of vendor 2 without rate code 2 | 468,855 | 518,863 |
| delete_partition `vendor_id=2` | 468,855 | (partition removed) |

The upsert leaves the counts unchanged because its rows are updates to existing trips, not new
records. The script's comments show the full `show()` output for each step.

## 2. HoodieStreamer

> **Status:** the configuration below matches Chapter 3 and Hudi 1.2.0. The expected results are
> derived from the input data and the Hudi 1.2.0 source code, but they are pending an end-to-end
> run.

### What is in the archive

`hudi_streamer.tar.gz` extracts to `/tmp/hudi_streamer`:

```
/tmp/hudi_streamer/
  streamer_input/    ten Parquet files of NYC taxi trips, 7,012,698 rows in total
  streamer_props/    input.props (record key and partition field)
  streamer_schema/   ny.avsc (Avro schema of the input)
```

### Run it

```bash
export SPARK_HOME=/path/to/spark-3.5.6-bin-hadoop3
./chapter03/run_hudi_streamer.sh
```

The script extracts the archive (if needed), downloads the two Hudi 1.2.0 JARs into
`/tmp/hudi_bundles`, prepares the input, and starts HoodieStreamer in continuous mode, writing to
`/tmp/hudi-deltastreamer-ny`. It refuses to start if that table already exists:

```bash
rm -rf /tmp/hudi-deltastreamer-ny
```

Two things the script does that are worth knowing:

- **It gives each input file its own modification time.** HoodieStreamer's DFS source reads files
  oldest first and stops a batch at `--source-limit` (30 MB, about one file) only when the next
  file is newer. Files extracted from an archive share one timestamp, so without this step the
  first batch would read all ten files.
- **It adds a copy of the first file as the fifth batch.** This simulates an upstream job
  delivering the same data twice. Because the record key identifies each trip, the replayed rows
  become updates rather than duplicates. On a Merge-on-Read table, updates are written to log
  files, which gives the asynchronous compaction something to merge.

### Key choices

- **Record key:** `VendorID,tpep_pickup_datetime,PULocationID,DOLocationID,trip_distance,total_amount`.
  The dataset has no trip ID column. A single column such as `VendorID` (only three values) would
  collapse the 7,012,698 rows into 994 records. The six-column key leaves only two collisions in
  the sample, and both are copies of the same trip.
- **Ordering field:** `--source-ordering-fields tpep_dropoff_datetime`. When two versions of a
  record share a key, the one with the later drop-off time wins; on a tie, the later write wins.

### Expected results (pending verification)

Stop the streamer with Ctrl+C once 11 delta commits have completed. In
`/tmp/hudi-deltastreamer-ny/.hoodie/timeline/` you should see:

- 11 `deltacommit` instants, one per input file (including the replay)
- a `compaction` instant scheduled after the fifth delta commit, completing while later batches
  are still being ingested

Then check the data in spark-shell:

```scala
val t = spark.read.format("hudi").load("file:///tmp/hudi-deltastreamer-ny")
t.count()
// expected: 7012696 (7,012,698 input rows minus the two duplicate copies; the replay adds none)

t.where("VendorID = 1 and tpep_pickup_datetime = '2019-08-20 09:16:13'").
  select("tpep_pickup_datetime", "tpep_dropoff_datetime").show()
// expected: one row with drop-off 2019-08-20 09:41:11. This trip arrives twice, with 09:41:11
// in the third batch and 09:40:33 in the sixth, and the ordering field keeps the later
// drop-off time even though it was written first.
```

## Files

- `hudi_write_operation_tutorial.scala`: write operations tutorial, with expected outputs in comments
- `run_hudi_streamer.sh`: HoodieStreamer setup and launch
- `hudi_streamer.tar.gz`: HoodieStreamer input data, properties file and schema (Git LFS)
- `README.md`: this guide

## Further reading

- [Hudi write operations](https://hudi.apache.org/docs/write_operations)
- [HoodieStreamer](https://hudi.apache.org/docs/hoodie_streaming_ingestion)
- [Hudi configurations](https://hudi.apache.org/docs/configurations)
