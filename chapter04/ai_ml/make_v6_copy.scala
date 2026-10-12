/**
 * Writes a copy of the store_sales table with Hudi table version 6, for readers that do not yet
 * support newer table versions (Daft, Amazon Athena, Apache XTable 0.4.0).
 *
 *   scala> :paste /path/to/hudiinaction/chapter04/ai_ml/make_v6_copy.scala
 */
import org.apache.spark.sql.SaveMode

val inputPath = "/tmp/input_data"
val raw = spark.read.parquet(inputPath)
val df  = raw.drop(raw.columns.filter(_.startsWith("_hoodie_")): _*)

df.write.format("hudi")
  .option("hoodie.table.name", "hudi_demo_tbl_v6")
  .option("hoodie.datasource.write.recordkey.field", "ss_item_sk,ss_ticket_number")
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .option("hoodie.write.table.version", "6")
  .mode(SaveMode.Overwrite)
  .save("/tmp/hudi_demo_tbl_v6")
