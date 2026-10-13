/**
 * Chapter 4: creating the store_sales Hudi table and reading it with the Spark DataSource API
 * and Spark Structured Streaming.
 *
 * Dataset: TPC-DS store_sales sample (2,880,404 rows) in chapter04/input_data.
 * Record key: (ss_item_sk, ss_ticket_number), the store_sales primary key.
 *
 * Run inside spark-shell (see README.md for the launch command):
 *   scala> :paste /path/to/hudiinaction/chapter04/hudi_querying_tutorial.scala
 */

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

// Path to chapter04/input_data in your clone of the repository.
val inputPath = "/tmp/input_data"
// Where the Hudi table is created. Must start empty.
val basePath  = "/tmp/hudi_demo_tbl"

// ============================================================================
// SECTION 1: Create the Hudi table with bulk_insert
// ============================================================================

// The sample Parquet files were exported from an older Hudi table and still carry
// _hoodie_* metadata columns, so drop them before writing.
val raw = spark.read.parquet(inputPath)
val df  = raw.drop(raw.columns.filter(_.startsWith("_hoodie_")): _*)

// No single store_sales column is unique: ss_item_sk has only 18,000 distinct values.
// The primary key is (ss_item_sk, ss_ticket_number), so both fields form the record key.
val writeOpts = Map(
  "hoodie.table.name" -> "hudi_demo_tbl",
  "hoodie.datasource.write.recordkey.field" -> "ss_item_sk,ss_ticket_number",
  "hoodie.datasource.write.operation" -> "bulk_insert")

df.write.format("hudi").options(writeOpts).mode(SaveMode.Overwrite).save(basePath)

// Hudi stores a composite key as field:value pairs, for example
// ss_item_sk:406,ss_ticket_number:188525
spark.read.format("hudi").load(basePath).select("_hoodie_record_key").show(1, false)

// ============================================================================
// SECTION 2: Query through the Spark DataSource API
// ============================================================================

spark.read.format("hudi").load(basePath).createOrReplaceTempView("hudi_tbl")
spark.sql("select count(*) from hudi_tbl").show(false)
// +--------+
// |count(1)|
// +--------+
// |2880404 |
// +--------+

spark.sql("select count(*) from hudi_tbl where ss_store_sk = 7").show(false)
// +--------+
// |count(1)|
// +--------+
// |458194  |
// +--------+

// Open the Spark UI (http://localhost:4040), SQL / DataFrame tab, to see the scan statistics.

// ============================================================================
// SECTION 3: Query through Spark Structured Streaming
// ============================================================================

import org.apache.spark.sql.streaming.Trigger
import java.time.LocalDateTime

val sourceBasePath = "/tmp/hudi_demo_tbl"
// With no start offset set, the stream starts from the earliest commit on the timeline
// (hoodie.datasource.streaming.startOffset defaults to earliest), so batch 0 is the whole table.
val streamDF = spark.readStream
  .format("hudi")
  .load(sourceBasePath)

val query = streamDF.writeStream
  .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
    batchDF.persist()
    println(LocalDateTime.now() + " start consuming batch " + batchId)
    println("Total record count " + batchDF.count())
    batchDF.unpersist()
    println(LocalDateTime.now() + " finish")
  }
  .option("checkpointLocation", "/tmp/hudi_streaming_hudi/checkpoint/")
  .trigger(Trigger.ProcessingTime("2 minutes"))
  .start()

// Expected output for the first micro-batch:
//   2026-10-12T15:16:44.870 start consuming batch 0
//   Total record count 2880404
//   2026-10-12T15:16:50.752 finish
//
// The checkpoint directory records which commits the query has consumed; restarting with the
// same directory resumes after the last processed commit. Stop the stream with:
//   query.stop()
