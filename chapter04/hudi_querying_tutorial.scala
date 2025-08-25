/**
 * Apache Hudi tutorial .....
 * 
 * This script demonstrates different ... to be fixed.
 * 
 * Dataset: store_sales dataset from tpc-ds benchmarking 1GB dataset.
 * Format: parquet
 */

// ============================================================================
// CONFIGURATION - Update these paths for your environment
// ============================================================================

// The dataset for this chapter is store sales dataset from tpc-ds benchmark which is located at chapter04/input_data.
// Please change to the path where the source data is saved
val inputPath = "/tmp/input_data/"

// Please change to the path where the Hudi table will be created
val basePath  = "/tmp/hudi_demo_tbl"

// ============================================================================
// SECTION 1: Creating Hudi table
// ============================================================================

/**
 * Load the store_sales dataset from the input (located at chapter04/input_data)
 */
val df = spark.read.format("parquet").load(inputPath)

/**
 * Create a new table with bulk insert operation.
 * Note: We are explicitly setting shuffle parallelism to 50 so that 50 data files will be created.
 */
df.write.format("hudi").
  option("hoodie.datasource.write.recordkey.field","ss_item_sk").
  option("hoodie.table.name","hudi_demo_tbl").
  option("hoodie.datasource.write.operation","bulk_insert").
  option("hoodie.bulkinsert.shuffle.parallelism","50").
  save(basePath)

// ============================================================================
// SECTION 2: Querying tables pre clustering
// ============================================================================

spark.read.format(“hudi”).load(basePath).registerTempTable(“hudi_tbl”)
spark.sql("select * from hudi_tbl where ss_store_sk = 7").show(100, false)

//+-------------------+-------------------------+------------------+----------------------+-------------------------------------------------------------------------+---------------+---------------+----------+--------------+-----------+-----------+----------+-----------+-----------+----------------+-----------+-----------------+-------------+--------------+-------------------+------------------+---------------------+-----------------+----------+-------------+-----------+-------------------+-------------+
//|_hoodie_commit_time|_hoodie_commit_seqno     |_hoodie_record_key|_hoodie_partition_path|_hoodie_file_name                                                        |ss_sold_date_sk|ss_sold_time_sk|ss_item_sk|ss_customer_sk|ss_cdemo_sk|ss_hdemo_sk|ss_addr_sk|ss_store_sk|ss_promo_sk|ss_ticket_number|ss_quantity|ss_wholesale_cost|ss_list_price|ss_sales_price|ss_ext_discount_amt|ss_ext_sales_price|ss_ext_wholesale_cost|ss_ext_list_price|ss_ext_tax|ss_coupon_amt|ss_net_paid|ss_net_paid_inc_tax|ss_net_profit|
//+-------------------+-------------------------+------------------+----------------------+-------------------------------------------------------------------------+---------------+---------------+----------+--------------+-----------+-----------+----------+-----------+-----------+----------------+-----------+-----------------+-------------+--------------+-------------------+------------------+---------------------+-----------------+----------+-------------+-----------+-------------------+-------------+
//|20250822082845301  |20250822082845301_0_138  |13814             |                      |5c10512e-e340-4ee4-b539-0e60d71ed540-0_0-3758-0_20250822082845301.parquet|NULL           |NULL           |13814     |47971         |NULL       |6773       |30574     |7          |286        |34              |20         |22.82            |NULL         |6.74          |NULL               |134.80            |NULL                 |NULL             |NULL      |NULL         |NULL       |NULL               |NULL         |
//  |20250822082845301  |20250822082845301_0_301  |14059             |                      |5c10512e-e340-4ee4-b539-0e60d71ed540-0_0-3758-0_20250822082845301.parquet|NULL           |67194          |14059     |NULL          |743793     |33         |NULL      |7          |296        |92              |36         |NULL             |136.44       |NULL          |0.00               |2750.40           |2493.36              |4911.84          |55.00     |0.00         |2750.40    |2805.40            |257.04       |
//  |20250822082845301  |20250822082845301_0_482  |5059              |                      |5c10512e-e340-4ee4-b539-0e60d71ed540-0_0-3758-0_20250822082845301.parquet|NULL           |NULL           |5059      |83850         |NULL       |1095       |NULL      |7          |142        |138             |82         |85.30            |NULL         |NULL          |1283.13            |1336.60           |6994.60              |10281.98         |NULL      |1283.13      |NULL       |57.74              |NULL         |
//  |20250822082845301  |20250822082845301_0_490  |11177             |                      |5c10512e-e340-4ee4-b539-0e60d71ed540-0_0-3758-0_20250822082845301.parquet|NULL           |49689          |11177     |11738         |NULL       |NULL       |NULL      |7          |NULL       |140             |15         |34.78            |50.08        |NULL          |NULL               |435.60            |NULL                 |NULL             |NULL      |NULL         |NULL       |NULL               |-86.10       |
// ...
// ...

// After you run the query, open Spark UI → SQL and click the query to see the DAG visualization for query execution stats.

// ============================================================================
// SECTION 3: Clustering the table by sorting on query predicated column
// ============================================================================

/**
 * Execute a phoney upsert just to trigger clustering.
 *
 * Key Hudi options explained:
 * - hoodie.clustering.inline: Enables clustering.
 * - hoodie.clustering.inline.max.commits: defines the frequency of clustering.
 * - hoodie.clustering.plan.strategy.target.file.max.bytes: defines the max file size with clustering. We are setting
 * it to 10Mb.
 * - hoodie.clustering.plan.strategy.small.file.limit: defines the small file candidate limit. Any file less than 120Mb
 * will be considered as input candidates for clustering.
 * - hoodie.clustering.plan.strategy.sort.columns: defines the columns to be sorted with clustering.
 */
df.limit(1).write.format("hudi").
  option("hoodie.datasource.write.recordkey.field","ss_item_sk").
  option("hoodie.table.name","hudi_demo_tbl").
  option("hoodie.datasource.write.operation","upsert").
  option("hoodie.clustering.inline","true").
  option("hoodie.clustering.inline.max.commits","1").
  option("hoodie.clustering.plan.strategy.target.file.max.bytes","10485760").
  option("hoodie.clustering.plan.strategy.small.file.limit", "125829120").
  option("hoodie.clustering.plan.strategy.sort.columns","ss_store_sk").
  mode(Append).
  save(basePath)

// ============================================================================
// SECTION 4: Querying tables pre clustering
// ============================================================================

spark.read.format(“hudi”).load(basePath).registerTempTable(“hudi_tbl_post_clustering”)
spark.sql("select * from hudi_tbl_post_clustering where ss_store_sk = 7").show(100, false)

//+-------------------+-------------------------+------------------+----------------------+-------------------------------------------------------------------------+---------------+---------------+----------+--------------+-----------+-----------+----------+-----------+-----------+----------------+-----------+-----------------+-------------+--------------+-------------------+------------------+---------------------+-----------------+----------+-------------+-----------+-------------------+-------------+
//|_hoodie_commit_time|_hoodie_commit_seqno     |_hoodie_record_key|_hoodie_partition_path|_hoodie_file_name                                                        |ss_sold_date_sk|ss_sold_time_sk|ss_item_sk|ss_customer_sk|ss_cdemo_sk|ss_hdemo_sk|ss_addr_sk|ss_store_sk|ss_promo_sk|ss_ticket_number|ss_quantity|ss_wholesale_cost|ss_list_price|ss_sales_price|ss_ext_discount_amt|ss_ext_sales_price|ss_ext_wholesale_cost|ss_ext_list_price|ss_ext_tax|ss_coupon_amt|ss_net_paid|ss_net_paid_inc_tax|ss_net_profit|
//+-------------------+-------------------------+------------------+----------------------+-------------------------------------------------------------------------+---------------+---------------+----------+--------------+-----------+-----------+----------+-----------+-----------+----------------+-----------+-----------------+-------------+--------------+-------------------+------------------+---------------------+-----------------+----------+-------------+-----------+-------------------+-------------+
//|20250822082845301  |20250822082845301_0_138  |13814             |                      |5c10512e-e340-4ee4-b539-0e60d71ed540-0_0-3758-0_20250822082845301.parquet|NULL           |NULL           |13814     |47971         |NULL       |6773       |30574     |7          |286        |34              |20         |22.82            |NULL         |6.74          |NULL               |134.80            |NULL                 |NULL             |NULL      |NULL         |NULL       |NULL               |NULL         |
//  |20250822082845301  |20250822082845301_0_301  |14059             |                      |5c10512e-e340-4ee4-b539-0e60d71ed540-0_0-3758-0_20250822082845301.parquet|NULL           |67194          |14059     |NULL          |743793     |33         |NULL      |7          |296        |92              |36         |NULL             |136.44       |NULL          |0.00               |2750.40           |2493.36              |4911.84          |55.00     |0.00         |2750.40    |2805.40            |257.04       |
//  |20250822082845301  |20250822082845301_0_482  |5059              |                      |5c10512e-e340-4ee4-b539-0e60d71ed540-0_0-3758-0_20250822082845301.parquet|NULL           |NULL           |5059      |83850         |NULL       |1095       |NULL      |7          |142        |138             |82         |85.30            |NULL         |NULL          |1283.13            |1336.60           |6994.60              |10281.98         |NULL      |1283.13      |NULL       |57.74              |NULL         |
//  |20250822082845301  |20250822082845301_0_490  |11177             |                      |5c10512e-e340-4ee4-b539-0e60d71ed540-0_0-3758-0_20250822082845301.parquet|NULL           |49689          |11177     |11738         |NULL       |NULL       |NULL      |7          |NULL       |140             |15         |34.78            |50.08        |NULL          |NULL               |435.60            |NULL                 |NULL             |NULL      |NULL         |NULL       |NULL               |-86.10       |
// ...
// ...

// After you run the query, open Spark UI → SQL and click the query to see the DAG visualization for query execution stats.
// You can find the difference in number of files read before and after clustering (58 -> 2) and latency difference as well.

// Lets move on to explore how file level data skipping with Hudi.

// ============================================================================
// SECTION 5: Creating a new Hudi table
// ============================================================================

/**
 * Load the store_sales dataset from the input (located at chapter04/input_data)
 */
val df = spark.read.format("parquet").load(inputPath)

// Please change to the path where the Hudi table will be created
val basePath  = "/tmp/hudi_data_skipping_demo_tbl"

/**
 * Create a new table with bulk insert operation. Since the previous table is already clustering, we are re-creating a
 * new table again.
 * Note: We are explicitly setting shuffle parallelism to 50 so that 50 data files will be created.
 */
df.write.format("hudi").
  option("hoodie.datasource.write.recordkey.field","ss_item_sk").
  option("hoodie.table.name","hudi_demo_tbl").
  option("hoodie.datasource.write.operation","bulk_insert").
  option("hoodie.bulkinsert.shuffle.parallelism","50").
  save(basePath)

// ============================================================================
// SECTION 6: Querying tables without file level data skipping.
// ============================================================================

spark.read.format(“hudi”).option(“hoodie.enable.data.skipping”,”false”).load(“/tmp/hudi_data_skipping_demo_tbl”).registerTempTable(“data_skipping_demo_tbl”)
spark.sql("select count(*) from data_skipping_demo_tbl where ss_net_profit > 9000").show(false)

//  +--------+
//  |count(1)|
//  +--------+
//  |11      |
//  +--------+

// After you run the query, open Spark UI → SQL and click the query to see the DAG visualization for query execution stats.

// ============================================================================
// SECTION 7: Querying tables with file level data skipping.
// ============================================================================

spark.read.format(“hudi”).load(“/tmp/hudi_data_skipping_demo_tbl”).registerTempTable(“data_skipping_demo_tbl”)
spark.sql("select count(*) from data_skipping_demo_tbl where ss_net_profit > 9000").show(false)

//  +--------+
//  |count(1)|
//  +--------+
//  |11      |
//  +--------+

// After you run the query, open Spark UI → SQL and click the query to see the DAG visualization for query execution stats.
// You can find the difference in number of files read before and after clustering (58 -> 10) and latency difference as well.







