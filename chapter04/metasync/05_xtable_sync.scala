/*
 * 05_xtable_sync.scala — Use Apache XTable to expose the same Hudi table as Iceberg/Delta
 *
 * What you get
 * - On write, Hudi produces XTable metadata so Iceberg/Delta readers can query
 *   the underlying files (snapshot on CoW / RO on MOR).
 *
 * Prereqs
 * - Add XTable sync tool to the classpath (either via Hudi’s XTable integration or xtable bundle).
 * Docs: https://hudi.apache.org/docs/syncing_xtable/ + https://xtable.apache.org/
 */

/// spark-shell launcher (Hudi bundle is required; add xtable if running independently):
// spark-shell --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
//  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
//  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
//  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog

val inputPath = "/tmp/input_data/"
val basePath  = "file:///tmp/sales_hudi_xtable"
val tableName = "sales_hudi"

val raw = spark.read.format("parquet").load(inputPath)
val df  = raw.drop(raw.columns.filter(_.startsWith("_hoodie_")):_*)

import org.apache.spark.sql.SaveMode

val writeOpts = Map(
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
  "hoodie.datasource.write.recordkey.field" -> "ss_item_sk",
  "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
  "hoodie.datasource.write.operation" -> "bulk_insert",
  "hoodie.bulkinsert.shuffle.parallelism" -> "50",

  // Enable generic meta sync and route to XTable
  "hoodie.datasource.meta.sync.enable" -> "true",
  "hoodie.datasource.meta.sync.classes" -> "org.apache.hudi.sync.xtable.XTableSyncTool",
  // Choose target formats (comma-separated): iceberg, delta
  "hoodie.xtable.sync.target_table_formats" -> "iceberg,delta"
)

df.write.format("hudi").options(writeOpts).mode(SaveMode.Overwrite).save(basePath)

/*
----- Independent sync (CLI) -----
# XTable can be run independently using the XTable CLI tool.
# Download XTable (incubating) from Apache and run:
java -jar xtable-utilities-0.1.0-bundled.jar \
  --tableFormat HUDI \
  --tablePath file:///tmp/sales_hudi_xtable \
  --targetFormats ICEBERG,DELTA
*/

/*
After sync you can query with format-native readers (on Spark), e.g.:
spark.read.format("iceberg").load(basePath).show(5)
spark.read.format("delta").load(basePath).show(5)
*/
// XTable enables cross-format metadata for CoW snapshot and MOR read-optimized.

// READ (choose one engine you've added to Spark):
/*
-- ICEBERG (Hadoop catalog by path)
spark.read.format("iceberg").load(basePath).selectExpr("count(*) as rows").show(false)

-- DELTA (by path)
spark.read.format("delta").load(basePath).selectExpr("count(*) as rows").show(false)
*/
