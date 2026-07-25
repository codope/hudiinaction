/**
 * Apache Hudi Quickstart Tutorial
 *
 * This script demonstrates the core capabilities of Apache Hudi, including:
 * - Creating Copy-on-Write (CoW) and Merge-on-Read (MoR) tables
 * - Performing upserts and deletes on Hudi tables
 * - Different query types: snapshot, read-optimized, incremental, and time-travel
 * - Table maintenance operations: compaction, clustering, and cleaning
 *
 * Dataset: New York Taxi dataset sample (~1M rows)
 * Format: Tab-separated CSV with headers
 *
 * Requirements:
 *   - Spark 3.5.x with Hudi 1.2.0 bundle
 *   - Launch via: ./run_spark_shell.sh
 */

// ============================================================================
// CONFIGURATION - Update these paths for your environment
// ============================================================================

// The dataset for this chapter is New York Taxi dataset sample of one million rows.
// Extract the `chapter02/trips_0.gz` file to a location.
// Please change to the path where the source data is saved
val inputPath = "/Users/username/path/to/trips_0"

// Please change to the path where the Hudi table will be created
// This will be used for the Copy-on-Write table
val basePath  = "/tmp/trips_table"

// ============================================================================
// SECTION 1: DATA LOADING AND COPY-ON-WRITE TABLE CREATION
// ============================================================================

// Load the NYC taxi dataset from tab-separated CSV format.
// inferSchema samples the file to detect column types (fare_amount as double,
// trip_distance as double, etc.) instead of reading everything as strings.
// In production, define an explicit StructType for safety.
val df = spark.read.format("csv").
  option("header", "true").
  option("sep",    "\t").
  option("inferSchema", "true").
  load(inputPath).
  toDF()

df.printSchema()

// Create a Copy-on-Write (CoW) Hudi table.
// CoW rewrites entire base files on every update — optimized for read-heavy workloads.
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",     "trip_id")
  .option("hoodie.datasource.write.partitionpath.field", "vendor_id")
  .option("hoodie.datasource.write.hive_style_partitioning", "true")
  .option("hoodie.table.name",                     "nyc_taxi_trips")
  .mode("Overwrite")
  .save(basePath)

// ============================================================================
// SECTION 2: TABLE VERIFICATION AND UPSERT OPERATIONS
// ============================================================================

// Verify table creation with a snapshot query.
val snapshotDf = spark.read.format("hudi").load(basePath)
snapshotDf.count()
// [VERIFY] Record the exact count from your run. It should match the CSV line count minus header.

snapshotDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount")
  .filter("vendor_id = '1'").limit(2).show()

// Update an existing record: increase fare_amount by 20%.
import org.apache.spark.sql.types.DoubleType
val toUpsert = df.filter($"trip_id" === "1207977523").
  withColumn("fare_amount", $"fare_amount".cast(DoubleType) * 1.2)

// Perform the upsert. Use mode("Append") — not "Overwrite" — to add to the table.
toUpsert.write.format("hudi")
  .option("hoodie.datasource.write.operation", "upsert")
  .mode("Append")
  .save(basePath)

// Verify: fare_amount should now be 19.2 (16 * 1.2).
val snapshotDf2 = spark.read.format("hudi").load(basePath)
snapshotDf2.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount")
  .filter($"trip_id" === "1207977523").show()

// ============================================================================
// SECTION 3: DELETE OPERATIONS
// ============================================================================

// Delete records with very short trip distances (< 0.1 miles).
val toDelete = df.filter($"trip_distance".cast("double") < 0.1)
toDelete.count()

toDelete.select("trip_id", "vendor_id", "pickup_datetime", "trip_distance")
  .filter("vendor_id = '1'").limit(1).show()

// Perform a hard delete — records are removed from the latest snapshot.
toDelete.write.format("hudi")
  .option("hoodie.datasource.write.operation", "delete")
  .mode("Append")
  .save(basePath)

// Verify: total count should decrease and the deleted record should not appear.
val snapshotDf3 = spark.read.format("hudi").load(basePath)
snapshotDf3.count()

snapshotDf3.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount")
  .filter($"trip_id" === "1200001601").show()
// Expected: empty result — the record was deleted.

// ============================================================================
// SECTION 4: EXPLORING HUDI METADATA AND COMMIT TIMELINE
// ============================================================================

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import scala.collection.JavaConverters._

val meta = HoodieTableMetaClient.builder().setBasePath(basePath).
  setConf(new HadoopStorageConfiguration(spark.sessionState.newHadoopConf())).
  build()

// Retrieve completed instants. Each instant has two timestamps:
//   requestedTime — when the action was requested (used for ordering and as-of queries)
//   completionTime — when the action finished (used for incremental query bounds)
val instants = meta.getCommitsTimeline.filterCompletedInstants.
  getInstants.iterator().asScala.toList.sortBy(_.requestedTime())

instants.foreach { i =>
  println(s"  requested=${i.requestedTime()}  completed=${i.getCompletionTime}  action=${i.getAction}")
}
// Expected: 3 instants — initial commit, upsert commit, delete commit

val firstCommit = instants(0)
val upsertCommit = instants(1)
val deleteCommit = instants(2)

// ============================================================================
// SECTION 5: HUDI QUERY TYPES DEMONSTRATION
// ============================================================================

// Read-optimized query.
// For CoW: identical to snapshot (no log files exist).
// For MoR: returns only compacted base files — faster but may lag behind the latest writes.
val roDf = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "read_optimized").
  load(basePath)
roDf.count()

// Incremental query — returns changes between two completion-time bounds.
// Boundary semantics (Hudi 1.x, completion-time mode):
//   begin is exclusive (>), end is inclusive (<=).
val incrDf = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "incremental").
  option("hoodie.datasource.read.begin.instanttime", firstCommit.getCompletionTime).
  option("hoodie.datasource.read.end.instanttime",   deleteCommit.getCompletionTime).
  load(basePath)

incrDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").show(false)
// Returns the latest state of keys affected between firstCommit and deleteCommit.
// NOTE: This is a latest-state result, not a before/after change log.
// For full CDC semantics (operation type + pre-images), enable CDC on the table
// and use the CDC query format — covered in Chapter 4.

// Time-travel query — read the table as it existed at a past instant.
// Use requestedTime for the as-of bound.
val ttDf = spark.read.format("hudi").
  option("as.of.instant", upsertCommit.requestedTime()).
  load(basePath)
ttDf.count()

// The deleted record should still exist at this point in time.
ttDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount")
  .filter($"trip_id" === "1200001601").show()

// ============================================================================
// SECTION 6: MERGE-ON-READ (MoR) TABLE OPERATIONS
// ============================================================================

// Create a MoR table. Updates go to append-only log files instead of rewriting
// base files — much faster writes, but snapshot reads must merge logs at read time.
val morBasePath = "/tmp/trips_table_mor"
df.write.format("hudi").
  option("hoodie.datasource.write.recordkey.field",     "trip_id").
  option("hoodie.datasource.write.partitionpath.field", "vendor_id").
  option("hoodie.datasource.write.hive_style_partitioning", "true").
  option("hoodie.datasource.write.table.type",     "MERGE_ON_READ").
  option("hoodie.table.name",                 "nyc_taxi_trips_mor").
  mode("Overwrite").
  save(morBasePath)

// Upsert on MoR — writes go to log files, no base-file rewrite.
toUpsert.write.format("hudi")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.operation", "upsert")
  .mode("Append")
  .save(morBasePath)

// Delete on MoR — delete markers go to log files.
toDelete.write.format("hudi")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.operation", "delete")
  .mode("Append")
  .save(morBasePath)

// Snapshot query on MoR — merges base files + log files for the complete view.
val morSnapshotDf = spark.read.format("hudi").load(morBasePath)
morSnapshotDf.count()

// Read-optimized query on MoR — reads only compacted base files, skips logs.
// Count will differ from snapshot because updates/deletes in logs are not visible.
val morRoDf = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "read_optimized").
  load(morBasePath)
morRoDf.count()

// ============================================================================
// SECTION 7: TABLE MAINTENANCE OPERATIONS
// ============================================================================

// --- Compaction ---
// Compaction merges the current base file with its eligible log files, producing
// a new base-file version. It does NOT rewrite every historical slice.
// Inline compaction runs on the writer path and adds write latency — convenient
// for tutorials but not recommended for high-throughput production workloads.
toUpsert.write.format("hudi")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.operation", "upsert")
  .option("hoodie.compact.inline", "true")
  .option("hoodie.compact.inline.max.delta.commits", "1")
  .mode("Append")
  .save(morBasePath)

// After compaction, snapshot and read-optimized return the same logical rows.
val morSnapshotDf2 = spark.read.format("hudi").load(morBasePath)
morSnapshotDf2.count()

val morRoDf2 = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "read_optimized").
  load(morBasePath)
morRoDf2.count()

// --- Clustering ---
// Clustering reorganizes files: combines small files into larger ones and
// optionally sorts by columns for better data skipping during queries.
df.limit(0).write.format("hudi")
  .option("hoodie.datasource.write.operation",                    "upsert")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.clustering.inline",                             "true")
  .option("hoodie.clustering.inline.max.commits",                 "1")
  .option("hoodie.clustering.plan.strategy.small.file.limit",     "10485760")
  .option("hoodie.clustering.plan.strategy.target.file.max.bytes","41943040")
  .option("hoodie.clustering.plan.strategy.sort.columns",         "pickup_date")
  .mode("Append")
  .save(morBasePath)

// --- Cleaning ---
// Cleaning removes older file-slice versions that fall outside the retention policy.
// These older slices are valid MVCC history (time travel, rollback, reader isolation)
// until the retention window expires. Aggressive cleaning shortens your time-travel
// and incremental-query window.
//
// Default behavior: cleaning runs inline with the writer automatically.
// Compaction and clustering do NOT run inline by default — they require explicit config.
df.limit(0).write.format("hudi")
  .option("hoodie.datasource.write.operation", "upsert")
  .option("hoodie.clean.commits.retained",     "1")
  .option("hoodie.clean.automatic",            "true")
  .mode("Append")
  .save(morBasePath)
