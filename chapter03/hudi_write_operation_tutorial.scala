/**
 * Apache Hudi tutorial on write operations (Chapter 3)
 *
 * Demonstrates, in the order the chapter introduces them:
 *   bulk_insert (with sort modes), insert, upsert, delete,
 *   insert_overwrite_table, insert_overwrite, delete_partition
 *
 * Tested with Spark 3.5.6, Java 11 and Hudi 1.2.0, starting from an empty base path.
 * The expected outputs in the comments come from that run.
 *
 * Dataset: NYC Taxi sample from Chapter 2 (chapter02/trips_0.gz), 1,000,660 rows,
 * tab-separated with a header row.
 *
 * Run it in spark-shell with `:paste hudi_write_operation_tutorial.scala`, or paste one section
 * at a time after typing `:paste`. Paste mode is needed because the multi-line snippets start
 * continuation lines with a dot.
 */

import org.apache.spark.sql.functions._

// ============================================================================
// CONFIGURATION - update these paths for your environment
// ============================================================================

// Decompress chapter02/trips_0.gz first (gunzip -k chapter02/trips_0.gz)
val inputPath = "/path/to/hudiinaction/chapter02/trips_0"

// The examples assume this path starts empty. To start over: rm -rf /tmp/trips_table
val basePath  = "/tmp/trips_table"

// Options shared by every write to this table. Each example adds only its write operation.
val hudiOptions = Map(
  "hoodie.table.name"                               -> "nyc_taxi_trips",
  "hoodie.datasource.write.recordkey.field"         -> "trip_id",
  "hoodie.datasource.write.partitionpath.field"     -> "vendor_id",
  "hoodie.datasource.write.hive_style_partitioning" -> "true"
)

// Row count per partition, run after each write to confirm what changed.
def showCounts(): Unit =
  spark.read.format("hudi").load(basePath).groupBy("vendor_id").count().orderBy("vendor_id").show()

val df = spark.read.format("csv").
  option("header", "true").
  option("sep", "\t").
  option("inferSchema", "true").
  load(inputPath)

df.count()
// res: Long = 1000660

// ============================================================================
// SECTION 1: BULK_INSERT (default sort mode NONE)
// ============================================================================
// Fastest way to load data: no index lookup and no small-file handling.
// Use it for the initial load.

df.write.format("hudi")
  .options(hudiOptions)
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .mode("Overwrite")
  .save(basePath)

showCounts()
// +---------+------+
// |vendor_id| count|
// +---------+------+
// |        1|468855|
// |        2|531805|
// +---------+------+

// ============================================================================
// SECTION 2: BULK_INSERT sort modes
// ============================================================================
// hoodie.bulkinsert.sort.mode decides how records are arranged before writing.
// In the tested run with four Spark tasks (local[4]):
//   NONE        -> 8 base files (four tasks x two partitions)
//   GLOBAL_SORT -> 5 base files
// File counts depend on the number of Spark tasks; row counts are unchanged.

df.write.format("hudi")
  .options(hudiOptions)
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .option("hoodie.bulkinsert.sort.mode", "NONE")
  .mode("Overwrite")
  .save(basePath)

df.write.format("hudi")
  .options(hudiOptions)
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .option("hoodie.bulkinsert.sort.mode", "GLOBAL_SORT")
  .mode("Overwrite")
  .save(basePath)

showCounts()
// Same counts as Section 1: 468855 and 531805

// ============================================================================
// SECTION 3: INSERT a new batch
// ============================================================================
// INSERT skips the index lookup, so it writes every record it is given, including records whose
// key already exists. Unlike BULK_INSERT, it packs new records into existing small files.
// To simulate a new batch of trips, we take 10,000 trips and give them new IDs.

val newTrips = df.orderBy("trip_id").limit(10000).withColumn("trip_id", col("trip_id") - 1000000000)

newTrips.write.format("hudi")
  .options(hudiOptions)
  .option("hoodie.datasource.write.operation", "insert")
  .mode("Append")
  .save(basePath)

// Shorter form, also tested: after the first write, the record key, partition field and table
// name are stored in .hoodie/hoodie.properties, so only the operation is needed:
//   newTrips.write.format("hudi").option("hoodie.datasource.write.operation", "insert").mode("Append").save(basePath)

showCounts()
// +---------+------+
// |vendor_id| count|
// +---------+------+
// |        1|473636|
// |        2|537024|
// +---------+------+
// 10,000 rows added. In the tested run the 10,000 rows went into the existing 5 file groups
// (new file slices), not into new file groups.

// ============================================================================
// SECTION 4: UPSERT updates to existing records
// ============================================================================
// UPSERT (the default operation) looks up every incoming key in the index, so records that
// already exist are updated rather than duplicated.
// To simulate corrections, we raise the tip on 1,000 existing trips.

val updatedTrips = df.orderBy("trip_id").limit(1000).withColumn("tip_amount", col("tip_amount") + 1)

updatedTrips.write.format("hudi")
  .options(hudiOptions)
  .option("hoodie.datasource.write.operation", "upsert")
  .mode("Append")
  .save(basePath)

// Shorter form, also tested: upsert is the default operation, so this works too:
//   updatedTrips.write.format("hudi").mode("Append").save(basePath)

showCounts()
// Counts unchanged (473636 and 537024): the 1,000 rows were updates, not new records.

spark.read.format("hudi").load(basePath).where(col("trip_id") === 1199999902).select("trip_id", "tip_amount").show()
// +----------+----------+
// |   trip_id|tip_amount|
// +----------+----------+
// |1199999902|      4.26|
// +----------+----------+
// The source value was 3.26.

// ============================================================================
// SECTION 5: DELETE records
// ============================================================================
// The DataFrame identifies the records to delete by their keys.

val tripsToDelete = df.where(col("rate_code_id") === 6)
tripsToDelete.count()
// res: Long = 8

tripsToDelete.write.format("hudi")
  .options(hudiOptions)
  .option("hoodie.datasource.write.operation", "delete")
  .mode("Append")
  .save(basePath)

showCounts()
// +---------+------+
// |vendor_id| count|
// +---------+------+
// |        1|473628|
// |        2|537024|
// +---------+------+
// 8 rows removed, all in vendor 1.

// ============================================================================
// SECTION 6: INSERT_OVERWRITE_TABLE
// ============================================================================
// Replaces the entire table with the incoming data. Recorded as a replacecommit; the old files
// stay on storage (readable by time travel) until the cleaner removes them.

df.write.format("hudi")
  .options(hudiOptions)
  .option("hoodie.datasource.write.operation", "insert_overwrite_table")
  .mode("Append")
  .save(basePath)

showCounts()
// Back to the full input: 468855 and 531805

// ============================================================================
// SECTION 7: INSERT_OVERWRITE
// ============================================================================
// Replaces only the partitions present in the incoming DataFrame; other partitions are unchanged.
// To replace partition vendor_id=2, we write its trips minus those with rate code 2.

val vendor2Trips = df.where(col("vendor_id") === 2 && col("rate_code_id") =!= 2)
vendor2Trips.count()
// res: Long = 518863

vendor2Trips.write.format("hudi")
  .options(hudiOptions)
  .option("hoodie.datasource.write.operation", "insert_overwrite")
  .mode("Append")
  .save(basePath)

showCounts()
// +---------+------+
// |vendor_id| count|
// +---------+------+
// |        1|468855|
// |        2|518863|
// +---------+------+
// Only vendor 2 was replaced; vendor 1 is unchanged.

// ============================================================================
// SECTION 8: DELETE_PARTITION
// ============================================================================
// Deletes whole partitions. hoodie.datasource.write.partitions.to.delete takes a comma-separated
// list of partition paths, and an entry may use * as a wildcard (not a regular expression).
// When this option is set, the DataFrame can be empty.

df.limit(0).write.format("hudi")
  .options(hudiOptions)
  .option("hoodie.datasource.write.operation", "delete_partition")
  .option("hoodie.datasource.write.partitions.to.delete", "vendor_id=2")
  .mode("Append")
  .save(basePath)

showCounts()
// +---------+------+
// |vendor_id| count|
// +---------+------+
// |        1|468855|
// +---------+------+

// Two behaviours confirmed in the tested run:
// - An entry that matches no partition, such as "vendor_id=3", is skipped without an error.
//   The commit succeeds but deletes nothing, so check the result after running it.
// - A wildcard entry such as "vendor_id=1*" deletes every partition it matches, here vendor_id=1.
