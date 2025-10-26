/*
 * 04_datahub_sync.scala — Sync a Hudi table's metadata to DataHub (governance/lineage)
 *
 * Prereqs (OSS quickstart)
 * - Install DataHub locally: `pip install acryl-datahub && datahub quickstart`
 *   This starts GMS/Frontend/Kafka containers and seeds a demo instance.
 * - Get your GMS host/port (defaults: http://localhost:8080)
 * Docs: https://hudi.apache.org/docs/syncing_datahub/
 */

/// spark-shell launcher:
// spark-shell --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
//  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
//  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
//  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog

val inputPath = "/tmp/input_data/"
val basePath  = "file:///tmp/sales_hudi_datahub"
val dbName    = "hudi_db"
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

  // Enable meta sync to DataHub via sync class
  "hoodie.datasource.meta.sync.enable" -> "true",
  "hoodie.datasource.meta.sync.classes" -> "org.apache.hudi.sync.datahub.DataHubSyncTool",
  // REST endpoint for GMS (property name varies slightly across versions; both forms shown)
  "hoodie.datahub.sync.server" -> "http://localhost:8080",
  // If your version uses host/port keys, uncomment these:
  // "hoodie.datahub.sync.rest.server.host" -> "localhost",
  // "hoodie.datahub.sync.rest.server.port" -> "8080",

  // Useful to populate names in DataHub’s dataset entity
  "hoodie.datasource.hive_sync.database" -> dbName,
  "hoodie.datasource.hive_sync.table"    -> tableName
)

df.write.format("hudi").options(writeOpts).mode(SaveMode.Overwrite).save(basePath)

/*
----- Independent sync (CLI) -----
# Using the Hudi sync tool script:
sh /path/to/run_sync_tool.sh \
  --sync-mode datahub \
  --base-path file:///tmp/sales_hudi_datahub \
  --database hudi_db \
  --table sales_hudi \
  --datahub-server-url http://localhost:8080
*/

/*
DataHub emits dataset metadata (schema, storage path, last sync). Verify in UI:
http://localhost:9002 → Search "sales_hudi"
*/
// DataHub sync support landed in Hudi 0.11 and remains available in 1.0.x.

// READ (Spark — path or register temp view)
val t = spark.read.format("hudi").load(basePath)
t.createOrReplaceTempView("sales_hudi_local")
spark.sql("SELECT ss_item_sk, SUM(ss_net_paid) AS net_paid FROM sales_hudi_local GROUP BY ss_item_sk LIMIT 10").show(false)

/*
Open DataHub UI (http://localhost:9002) and search "sales_hudi" to see schema + last sync.
*/
