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
 */

// Required imports for Spark SQL operations and Hudi functionality
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.types.DoubleType
import spark.implicits._

// ============================================================================
// CONFIGURATION - Update these paths for your environment
// ============================================================================

// The dataset for this chapter is New York Taxi dataset sample of one million rows.
// Untar the `chapter02/trips_0.gz` file to a location.
// Please change to the path where the source data is saved
val inputPath = "/Users/username/path/to/trips_0"

// Please change to the path where the Hudi table will be created
// This will be used for the Copy-on-Write table
val basePath  = "/tmp/trips_table"

// ============================================================================
// SECTION 1: DATA LOADING AND COPY-ON-WRITE TABLE CREATION
// ============================================================================

/**
 * Load the NYC taxi dataset from CSV format
 * The dataset is tab-separated with headers containing taxi trip information
 */
val df = spark.read.format("csv").
  option("header", "true").
  option("sep",    "\t").     // Tab-separated values
  load(inputPath).
  toDF()

/**
 * Create a Copy-on-Write (CoW) Hudi Table
 * 
 * CoW tables provide:
 * - Excellent query performance (no merge required during read)
 * - Higher write latency due to file rewriting
 * - Suitable for read-heavy workloads
 * 
 * Key Hudi options explained:
 * - recordkey.field: Unique identifier for each record (trip_id)
 * - partitionpath.field: Field used for partitioning (vendor_id)
 * - precombine.field: Field used to resolve duplicates (pickup_datetime - latest wins)
 * - hive_style_partitioning: Creates Hive-compatible partition structure
 * - table.name: Logical name for the Hudi table
 */
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",     "trip_id")
  .option("hoodie.datasource.write.partitionpath.field", "vendor_id")
  .option("hoodie.datasource.write.precombine.field",    "pickup_datetime")
  .option("hoodie.datasource.write.hive_style_partitioning", "true")
  .option("hoodie.table.name",                     "nyc_taxi_trips")
  .mode("Overwrite")  // Initial table creation
  .save(basePath)

// ============================================================================
// SECTION 2: TABLE VERIFICATION AND UPSERT OPERATIONS
// ============================================================================

/**
 * Verify table creation with a snapshot query
 * Snapshot queries return the latest committed data across all partitions
 */
val snapshotDf = spark.read.format("hudi").load(basePath)
snapshotDf.count() // Expected: ~1000660 rows
snapshotDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").filter("vendor_id = '1'").limit(2).show()
// Expected output:
// +----------+---------+-------------------+-----------+
// |   trip_id|vendor_id|    pickup_datetime|fare_amount|
// +----------+---------+-------------------+-----------+
// |1207977523|        1|2015-08-10 10:36:34|         16|
// |1208792114|        1|2015-08-11 06:17:47|        9.5|
// +----------+---------+-------------------+-----------+

/**
 * Demonstrate Upsert (Update + Insert) Operation
 * 
 * We'll update an existing record by:
 * 1. Increasing the fare amount by 20% (multiply by 1.2)
 * 2. Adding 1 hour (3600 seconds) to the pickup time
 * 
 * Upserts are key to Hudi's functionality - they allow you to update existing
 * records or insert new ones in a single operation
 */
val toUpsert = df.filter($"trip_id" === "1207977523").
  withColumn("fare_amount", $"fare_amount".cast(DoubleType) * 1.2).
  withColumn("pickup_datetime",   from_unixtime(unix_timestamp($"pickup_datetime") + 3600))

/**
 * Perform the upsert operation
 * Note: We use mode("Append") for upserts, not mode("Overwrite")
 */
toUpsert.write.format("hudi")
  .option("hoodie.datasource.write.operation",  "upsert")
  .mode("Append")
  .save(basePath)

/**
 * Verify the upsert operation by reading the updated record
 * Notice the fare_amount changed from 16 to 19.2 (16 * 1.2)
 * and pickup_datetime shifted by 1 hour
 */
val snapshotDf = spark.read.format("hudi").load(basePath)
snapshotDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").filter($"trip_id" === "1207977523").show()
// Expected output after upsert:
// +----------+---------+-------------------+-----------+
// |   trip_id|vendor_id|    pickup_datetime|fare_amount|
// +----------+---------+-------------------+-----------+
// |1207977523|        1|2015-08-10 11:36:34|       19.2|
// +----------+---------+-------------------+-----------+

// ============================================================================
// SECTION 3: DELETE OPERATIONS
// ============================================================================

/**
 * Demonstrate Delete Operation
 * 
 * We'll delete records with very short trip distances (< 0.1 miles)
 * These might represent invalid or test data
 */
val toDelete = df.filter($"trip_distance".cast("double") < 0.1)
toDelete.count() // Expected: ~7900 rows to be deleted

// Preview some records that will be deleted
toDelete.select("trip_id", "vendor_id", "pickup_datetime", "trip_distance").filter("vendor_id = '1'").limit(1).show()
// Expected output - sample record to be deleted:
// +----------+---------+-------------------+-------------+
// |   trip_id|vendor_id|    pickup_datetime|trip_distance|
// +----------+---------+-------------------+-------------+
// |1200001601|        1|2015-07-16 23:38:37|            0|
// +----------+---------+-------------------+-------------+

/**
 * Perform the delete operation
 * Hudi supports hard deletes - records are completely removed from the table
 */
toDelete.write.format("hudi")
  .option("hoodie.datasource.write.operation", "delete") // Specify delete operation
  .mode("Append")
  .save(basePath)

/**
 * Verify the delete operation
 * The total row count should decrease by the number of deleted records
 */
val snapshotDf = spark.read.format("hudi").load(basePath)
snapshotDf.count() // Expected: 1000660 - 7900 = 992760 rows

// Verify that the specific record we saw earlier was actually deleted
snapshotDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").filter($"trip_id" === "1200001601").show()
// Expected output (empty result - record was deleted):
// +-------+---------+---------------+-----------+
// |trip_id|vendor_id|pickup_datetime|fare_amount|
// +-------+---------+---------------+-----------+
// +-------+---------+---------------+-----------+

// ============================================================================
// SECTION 4: EXPLORING HUDI METADATA AND COMMIT TIMELINE
// ============================================================================

/**
 * Import Hudi classes for accessing table metadata
 * These allow us to explore the commit timeline and table history
 */
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import scala.collection.JavaConversions._

/**
 * Build a Hudi meta client to access table metadata
 * This provides access to the commit timeline, schema evolution, and other metadata
 */
val meta = HoodieTableMetaClient.builder().setBasePath(basePath).
  setConf(new HadoopStorageConfiguration(spark.sessionState.newHadoopConf())).
  build()

/**
 * Retrieve the commit timeline
 * Hudi maintains a timeline of all operations (commits) performed on the table
 * Each operation gets a unique timestamp identifier
 */
val commits = meta.getCommitsTimeline.filterCompletedInstants.
  getInstants.toList.map(_.getCompletionTime()).sorted

println(commits.mkString("Commits -> ", ", ", ""))
// Expected output (timestamps will differ):
// Commits -> 20250622072149247, 20250622074915924, 20250622082539914

/**
 * Extract individual commit timestamps for use in queries
 * We expect 3 commits: initial load, upsert, and delete
 */
val firstCommit = commits.get(0)   // Initial table creation
val upsertCommit = commits.get(1)  // Record update operation
val deleteCommit = commits.get(2)  // Record deletion operation

// ============================================================================
// SECTION 5: HUDI QUERY TYPES DEMONSTRATION
// ============================================================================

/**
 * Read-Optimized Query
 * 
 * For CoW tables: Same as snapshot query (reads latest committed data)
 * For MoR tables: Only reads base files, ignoring recent updates in log files
 * This provides faster query performance but may not include the latest changes
 */
val roDf = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "read_optimized").
  load(basePath)
roDf.count()  // Expected: 992760 rows (same as snapshot for CoW, different for MoR)

/**
 * Incremental Query
 * 
 * Reads only the changes between two specific commit times
 * Very useful for building data pipelines and processing only new/changed data
 * This is one of Hudi's most powerful features for incremental data processing
 */
val incrDf = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "incremental").
  option("hoodie.datasource.read.begin.instanttime", upsertCommit).  // Start from this commit
  option("hoodie.datasource.read.end.instanttime",   deleteCommit).  // Up to (but not including) this commit
  load(basePath)

// IMPORTANT: Begin instant time is INCLUSIVE, end instant time is EXCLUSIVE
// This query will return records changed between upsertCommit and deleteCommit
incrDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").show(false)
// Expected output (the record we updated):
// +----------+---------+-------------------+-----------+
// |trip_id   |vendor_id|pickup_datetime    |fare_amount|
// +----------+---------+-------------------+-----------+
// |1207977523|1        |2015-08-10 11:36:34|19.2       |
// +----------+---------+-------------------+-----------+

/**
 * Time Travel Query
 * 
 * Query the table as it existed at a specific point in time
 * This allows you to see historical data states and is perfect for:
 * - Auditing and compliance
 * - Debugging data issues
 * - Creating point-in-time reports
 */
val ttDf = spark.read.format("hudi").
  option("as.of.instant", upsertCommit).   // Query table state at this specific commit
  load("/tmp/trips_table")
ttDf.count()                // Expected: 1000660 (back to row count before delete operation)

// Verify that a record deleted later still exists at this point in time
ttDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").filter($"trip_id" === "1200001601").show()
// Expected output (record exists in this historical view):
// +----------+---------+-------------------+-----------+
// |   trip_id|vendor_id|    pickup_datetime|fare_amount|
// +----------+---------+-------------------+-----------+
// |1200001601|        1|2015-07-16 23:38:37|          3|
// +----------+---------+-------------------+-----------+

// ============================================================================
// SECTION 6: MERGE-ON-READ (MoR) TABLE OPERATIONS
// ============================================================================

/**
 * Create a Merge-on-Read (MoR) Table
 * 
 * MoR tables provide:
 * - Faster write performance (updates go to log files)
 * - Potentially slower read performance (requires merging base + log files)
 * - Suitable for write-heavy workloads
 * - Requires periodic compaction for optimal read performance
 */
val morBasePath = "/tmp/trips_table_mor"
df.write.format("hudi").
  option("hoodie.datasource.write.recordkey.field",     "trip_id").
  option("hoodie.datasource.write.partitionpath.field", "vendor_id").
  option("hoodie.datasource.write.precombine.field",    "pickup_datetime").
  option("hoodie.datasource.write.hive_style_partitioning", "true").
  // KEY DIFFERENCE: Set table type to MERGE_ON_READ
  option("hoodie.datasource.write.table.type",     "MERGE_ON_READ").
  option("hoodie.table.name",                 "nyc_taxi_trips_mor").
  mode("Overwrite").
  save(morBasePath)

/**
 * Perform upsert on MoR table without compaction
 * Updates will be written to log files rather than rewriting base files
 */
toUpsert.write.format("hudi")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.operation", "upsert")
  .mode("Append")
  .save(morBasePath)

/**
 * Perform delete on MoR table without compaction
 * Delete markers will be written to log files
 */
toDelete.write.format("hudi")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.operation", "delete")
  .mode("Append")
  .save(morBasePath)

/**
 * Demonstrate the difference between query types on MoR tables
 * This is where MoR tables show their unique characteristics
 */

// Snapshot query on MoR - merges base files + log files for complete view
val snapshotDf = spark.read.format("hudi").load(morBasePath)
snapshotDf.count() // Expected: 992760 rows (includes all changes)

// Read-optimized query on MoR - only reads base files (faster but incomplete)
val roDf = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "read_optimized").
  load(morBasePath)
roDf.count() // Expected: 1000660 rows (original data, doesn't include updates/deletes in log files)

// ============================================================================
// SECTION 7: TABLE MAINTENANCE OPERATIONS
// ============================================================================

/**
 * Compaction Operation
 * 
 * Compaction merges base files with log files to improve read performance
 * on MoR tables. This is essential for maintaining good query performance.
 * 
 * Inline compaction triggers automatically based on configured thresholds.
 * Here we set it to trigger after just 1 delta commit for demonstration.
 */
toUpsert.write.format("hudi")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.operation", "upsert")
  .option("hoodie.compact.inline", "true")                    // Enable inline compaction
  .option("hoodie.compact.inline.max.delta.commits", "1")     // Trigger after 1 delta commit
  .mode("Append")
  .save(morBasePath)

/**
 * Clustering Operation
 * 
 * Clustering reorganizes data for better query performance by:
 * - Combining small files into larger files (reduces metadata overhead)
 * - Optionally sorting data by specified columns
 * - Improving data locality for range queries
 * 
 * This example clusters 10MB files into 40MB files and sorts by pickup_date
 */
df.limit(0).write.format("hudi")  // Empty DataFrame to trigger clustering only
  .option("hoodie.datasource.write.operation",                 "upsert")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.clustering.inline",                          "true")
  .option("hoodie.clustering.inline.max.commits",              "1")
  // File size thresholds for clustering
  .option("hoodie.clustering.plan.strategy.small.file.limit",  "10485760")    // 10MB threshold
  .option("hoodie.clustering.plan.strategy.target.file.max.bytes","41943040") // Target 40MB files
  // Sort data during clustering for better query performance
  .option("hoodie.clustering.plan.strategy.sort.columns",      "pickup_date")
  .mode("Append")
  .save(morBasePath)

/**
 * Cleaning Operation
 * 
 * Cleaning removes old file versions to reclaim storage space.
 * This is important for managing storage costs and maintaining performance.
 * 
 * The "commits.retained" setting determines how many previous versions to keep.
 * Setting it to "1" means only the current version is retained (aggressive cleaning).
 */
df.limit(0).write.format("hudi")  // Empty DataFrame to trigger cleaning only
  .option("hoodie.datasource.write.operation",    "upsert")
  .option("hoodie.clean.commits.retained",        "1")        // Keep only 1 previous commit
  .option("hoodie.clean.automatic",               "true")     // Enable automatic cleaning
  .mode("Append")
  .save(morBasePath)
