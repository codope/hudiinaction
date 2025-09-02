/**
 * Apache Hudi tutorial on querying Hudi tables.
 * 
 * This script demonstrates different ways ot read a Hudi table using spark.
 * 
 * Dataset: store_sales dataset from tpc-ds benchmarking 1GB dataset.
 * Format: parquet
 */

// ============================================================================
// CONFIGURATION - Update these paths for your environment
// ============================================================================

// input data is stored in chapter04/input_data/ in hudiinaction github repo. Feel free to replace below line or
// copy the contents of chapter04/input_data/ to /tmp/input_data/
val inputPath = "/tmp/input_data/"

// Launch spark-shell
./bin/spark-shell --driver-memory 4g --executor-memory 4g --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

// Required imports
import org.apache.spark.sql.SaveMode._

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
// SECTION 2: Querying tables via spark datasource.
// ============================================================================

spark.read.format(“hudi”).load(basePath).registerTempTable(“hudi_tbl”)
spark.sql("select count(*) from hudi_tbl where ss_store_sk = 7").show(false)

Output:
//  +--------+
//  |count(1)|
//  +--------+
//  |458194  |
//  +--------+

// After you run the query, open Spark UI → SQL tab and click the query to see the DAG visualization for query execution stats.

// ============================================================================
// SECTION 3: Querying hudi table via Spark SQL
// ============================================================================

// lets launch spark-sql session

./bin/spark-sql --driver-memory 4g --executor-memory 4g --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

// Lets drop the table if exists already
DROP TABLE IF EXISTS hudi_tbl;

// lets create an external table mapping to the hudi table location
CREATE TABLE hudi_tbl USING hudi LOCATION '/tmp/hudi_demo_tbl';

// Now, we are ready to query the hudi table via Spark SQL
SELECT count(*) FROM hudi_tbl where ss_store_sk = 7;

Output:
// 458194
// Time taken: 7.613 seconds, Fetched 1 row(s)

// ============================================================================
// SECTION 3: Querying hudi table via Spark Structured Streaming
// ============================================================================

// Launch spark-shell
./bin/spark-shell --driver-memory 4g --executor-memory 4g --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

// lets execute a streaming query to consume data from our hudi table using spark structured streaming

import org.apache.spark.sql.streaming.Trigger
import java.time.LocalDateTime
val sourceBasePath = "/tmp/hudi_demo_tbl"
val df = spark.readStream.format("hudi").load(sourceBasePath)
val query = df.writeStream.foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) => {
  batchDF.persist()
  println(LocalDateTime.now() + " start consuming batch " + batchId)
  println("Total record count " + batchDF.count())
  println(LocalDateTime.now() + " finish")
}
}.option("checkpointLocation", "/tmp/hudi_streaming_hudi/checkpoint/").
  trigger(Trigger.ProcessingTime("2 minutes")).start()

Output:
// 25/09/01 18:50:41 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
// 2025-09-01T18:50:41.498 start consuming batch 0
// Total record count 2880404
// 2025-09-01T18:51:01.645 finish

