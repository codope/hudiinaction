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
 * Execute bulk insert operation with default sort mode
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
 * Key Hudi options explained:
 * - bulkinsert.sort.mode: dictates the repartitioning strategy to use with bulk_insert operation
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
 * Key Hudi options explained:
 * - write.operation: dictates the write operation to use (delete)
 */
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",       "trip_id")
  .option("hoodie.datasource.write.partitionpath.field", "vendor_id")
  .option("hoodie.datasource.write.hive_style_partitioning",  "true")
  .option("hoodie.table.name",                      "nyc_taxi_trips")
  .option("hoodie.datasource.write.operation",         "delete")
  .mode("Append")
  .save(basePath)

// ============================================================================
// SECTION 6: INGESTING DATA VIA INSERT_OVERWRITE_TABLE OPERATION
// ============================================================================

/**
 * Execute insert_overwrite_table operation
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
 * Key Hudi options explained:
 * - write.operation: dictates the write operation to use (delete_partition)
 */
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",         "trip_id")
  .option("hoodie.datasource.write.partitionpath.field",   "vendor_id")
  .option("hoodie.datasource.write.hive_style_partitioning",    "true")
  .option("hoodie.table.name",                        "nyc_taxi_trips")
  .option("hoodie.datasource.write.operation",      "delete_partition")
  .option("hoodie.datasource.write.partitions.to.delete", "2025/01/01")
  .mode("Append")
  .save(basePath)
