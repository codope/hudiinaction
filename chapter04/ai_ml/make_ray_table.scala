/**
 * Writes a small Copy-on-Write copy of store_sales that Ray Data can read, then upserts 20 rows
 * so the incremental example has a second commit.
 *
 * Ray 2.59's read_hudi uses hudi-rs 0.4, which reads table version 6 only, and which finds no
 * files in a non-partitioned table written by Hudi 1.2.1. So this copy uses table version 6 and
 * is partitioned by ss_store_sk.
 *
 *   scala> :paste /path/to/hudiinaction/chapter04/ai_ml/make_ray_table.scala
 */
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

val path = "/tmp/ss_cow_v6_part"
val opts = Map(
  "hoodie.table.name" -> "ss_cow_v6_part",
  "hoodie.datasource.write.recordkey.field" -> "ss_item_sk,ss_ticket_number",
  "hoodie.datasource.write.partitionpath.field" -> "ss_store_sk",
  "hoodie.write.table.version" -> "6")

val raw = spark.read.parquet("/tmp/input_data")
val small = raw.drop(raw.columns.filter(_.startsWith("_hoodie_")): _*)
  .where("ss_store_sk in (1, 7)").limit(10000)
small.write.format("hudi").options(opts + ("hoodie.datasource.write.operation" -> "bulk_insert"))
  .mode(SaveMode.Overwrite).save(path)

val upd = spark.read.format("hudi").load(path)
  .orderBy(rand()).limit(20)
  .withColumn("ss_quantity", col("ss_quantity") + 1)
upd.write.format("hudi").options(opts + ("hoodie.datasource.write.operation" -> "upsert"))
  .mode("append").save(path)

// The two commit times, for the incremental read in ray_data_example.py
new java.io.File(path + "/.hoodie").listFiles.map(_.getName).filter(_.endsWith(".commit")).sorted.foreach(println)
