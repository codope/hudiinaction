/**
 * Apache Hudi tutorial on various write operations
 * 
 * This script demonstrates different write operations with Apache Hudi:
 * - Bulk_insert
 * - Insert
 * - Upsert
 * - Delete
 * - Insert_Overwrite_Table
 * - Insert_Overwrite
 * - Delete_Partitions
 * 
 * Dataset: New York Taxi dataset sample (~1M rows)
 * Format: Tab-separated CSV with headers
 */

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
// SECTION 1: DATA LOADING VIA BULK_INSERT OPERATION
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
 * Execute bulk insert operation with default sort mode.
 * Bulk insert is mostly used for initial bulk importing of data into your sink hudi table or for immutable ingests.
 * It offers one of the fastest way to ingest data into your lakehouse tables without any additional overhead like indexing
 * and small file management.
 * 
 * Key Hudi options explained:
 * - write.operation: dictates the write operation to use (bulk_insert)
 */
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",       "trip_id")
  .option("hoodie.datasource.write.partitionpath.field", "vendor_id")
  .option("hoodie.datasource.write.hive_style_partitioning",  "true")
  .option("hoodie.table.name",                      "nyc_taxi_trips")
  .option("hoodie.datasource.write.operation",         "bulk_insert")
  .mode("Overwrite")
  .save(basePath)

// ============================================================================
// SECTION 2: EMPLOY SORT MODE TO OPTIMIZE THE LAYOUT WITH BULK_INSERT
// ============================================================================

/**
 * Execute bulk insert operation with default sort mode
 *
 * Sort modes can have a lasting impact to your table layouts, small files and query latencies as well.
 * Please refer to chapter 03 from Hudi In Action to learn more about different sort modes.
 *
 * Key Hudi options explained:
 * - bulkinsert.sort.mode: dictates the repartitioning strategy to use with bulk_insert operation
 *
 */
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",       "trip_id")
  .option("hoodie.datasource.write.partitionpath.field", "vendor_id")
  .option("hoodie.datasource.write.hive_style_partitioning",  "true")
  .option("hoodie.table.name",                      "nyc_taxi_trips")
  .option("hoodie.datasource.write.operation",         "bulk_insert")
  .option("hoodie.bulkinsert.sort.mode",               "GLOBAL_SORT")
  .mode("Overwrite")
  .save(basePath)

// ============================================================================
// SECTION 3: INGESTING DATA VIA INSERT OPERATION
// ============================================================================

/**
 * Execute insert operation
 *
 * Once initial data is loaded via bulk_insert, one can use `insert` operation to ingest immutable data.
 * This operation nicely sizes the files and also manages small files.
 *
 * Key Hudi options explained:
 * - write.operation: dictates the write operation to use (insert)
 */
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",       "trip_id")
  .option("hoodie.datasource.write.partitionpath.field", "vendor_id")
  .option("hoodie.datasource.write.hive_style_partitioning",  "true")
  .option("hoodie.table.name",                      "nyc_taxi_trips")
  .option("hoodie.datasource.write.operation",         "insert")
  .mode("Append")
  .save(basePath)

// ============================================================================
// SECTION 4: INGESTING DATA VIA UPSERT OPERATION
// ============================================================================

/**
 * Execute upsert operation
 *
 * This is the default operation used in hudi for mutable datasets.
 * Both inserts and updates can be ingested using this operation.
 *
 * Key Hudi options explained:
 * - write.operation: Default operation type is upsert and hence the option is ignored.
 */
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",       "trip_id")
  .option("hoodie.datasource.write.partitionpath.field", "vendor_id")
  .option("hoodie.datasource.write.hive_style_partitioning",  "true")
  .option("hoodie.table.name",                      "nyc_taxi_trips")
  .mode("Append")
  .save(basePath)

// ============================================================================
// SECTION 5: DELETING DATA VIA DELETE OPERATION
// ============================================================================

/**
 * Execute delete operation
 *
 * We'll delete records with rate_code_id = "6".
 */

// We might need to prepare input dataset to delete before we can execute the delete operation.
df.registerTempTable("raw_tbl_view_batch1")
spark.sql("select rate_code_id, count(*) from raw_tbl_view_batch1 group by 1 order by 2").show(false)

//  +------------+--------+
//  |rate_code_id|count(1)|
//  +------------+--------+
//  |6           |8       |
//  |99          |54      |
//  |4           |477     |
//  |3           |1759    |
//  |5           |3235    |
//  |2           |23189   |
//  |1           |971938  |
//  +------------+--------+

// We have a total of 8 records with rate_code_id = "6"

// Lets prepare the dataframe to delete
val dfToDelete = df.where(col("rate_code_id") === '6')

dfToDelete.count
// res16: Long = 8

dfToDelete.write.format("hudi").
  option("hoodie.datasource.write.operation","delete").
  mode("Append").  // Subsequent operations to same hudi table should use Append mode
  save(basePath)

// lets query for all diff "rate_code_id"s to confirm that deletion has been successful.

val df2 = spark.read.format("hudi").load(basePath)
df2.registerTempTable("raw_tbl_view_batch2")

scala> spark.sql("select rate_code_id, count(*) from raw_tbl_view_batch2 group by 1 order by 2").show(false)
//  +------------+--------+
//  |rate_code_id|count(1)|
//  +------------+--------+
//  |99          |54      |
//  |4           |477     |
//  |3           |1759    |
//  |5           |3235    |
//  |2           |23189   |
//  |1           |971938  |
//  +------------+--------+

// all records with rate_code_id = "6" has been deleted now.

// ============================================================================
// SECTION 6: INGESTING DATA VIA INSERT_OVERWRITE_TABLE OPERATION
// ============================================================================

/**
 * Execute insert_overwrite_table operation
 *
 * Overwrites entire table with the incoming data.
 *
 * Key Hudi options explained:
 * - write.operation: dictates the write operation to use (insert_overwrite_table)
 */
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",       "trip_id")
  .option("hoodie.datasource.write.partitionpath.field", "vendor_id")
  .option("hoodie.datasource.write.hive_style_partitioning",  "true")
  .option("hoodie.table.name",                      "nyc_taxi_trips")
  .option("hoodie.datasource.write.operation",         "insert_overwrite_table")
  .mode("Append")
  .save(basePath)

// ============================================================================
// SECTION 7: INGESTING DATA VIA INSERT_OVERWRITE OPERATION
// ============================================================================

/**
 * Execute insert_overwrite_table operation
 *
 * Overwrites matching partitions with incoming data.
 *
 * Key Hudi options explained:
 * - write.operation: dictates the write operation to use (insert_overwrite)
 */
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",       "trip_id")
  .option("hoodie.datasource.write.partitionpath.field", "vendor_id")
  .option("hoodie.datasource.write.hive_style_partitioning",  "true")
  .option("hoodie.table.name",                      "nyc_taxi_trips")
  .option("hoodie.datasource.write.operation",         "insert_overwrite")
  .mode("Append")
  .save(basePath)

// ============================================================================
// SECTION 8: DELETING PARTITIONS DATA VIA DELETE_PARTITION OPERATION
// ============================================================================

/**
 * Execute delete_partition operation
 *
 * Deletes entire partitions in the given hudi table.
 *
 * Key Hudi options explained:
 * - write.operation: dictates the write operation to use (delete_partition)
 */

// lets query total records per partition before triggering delete_partition
spark.read.format("hudi").load(basePath).registerTempTable("tbl_temp_view_delete_partition")
spark.sql("select vendor_id, count(*) from tbl_temp_view_delete_partition group by 1").show(false)

//  +---------+--------+
//  |vendor_id|count(1)|
//  +---------+--------+
//  |2        |531805  |
//  |1        |468847  |
//  +---------+--------+

// lets delete partition "vendor_id=2"

df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",         "trip_id")
  .option("hoodie.datasource.write.partitionpath.field",   "vendor_id")
  .option("hoodie.datasource.write.hive_style_partitioning",    "true")
  .option("hoodie.table.name",                        "nyc_taxi_trips")
  .option("hoodie.datasource.write.operation",      "delete_partition")
  .option("hoodie.datasource.write.partitions.to.delete", "vendor_id=2")
  .mode("Append")
  .save(basePath)

// lets query the table again to check if deletion is successful.
spark.read.format("hudi").load(basePath).registerTempTable("tbl_temp_view_delete_partition")
spark.sql("select vendor_id, count(*) from tbl_temp_view_delete_partition group by 1").show(false)
//  +---------+--------+
//  |vendor_id|count(1)|
//  +---------+--------+
//  |1        |468847  |
//  +---------+--------+