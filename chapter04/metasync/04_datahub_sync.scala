/*
 * 04_datahub_sync.scala: publish a Hudi table's metadata to DataHub.
 *
 * Hudi 1.2.1, Spark 3.5. Docs: https://hudi.apache.org/docs/syncing_datahub/
 * Not run by the authors (needs a DataHub instance). Config keys checked against the 1.2.1 source.
 *
 * What the sync publishes: the dataset with its schema, table and partition properties, the
 * last commit time, browse paths and containers, and optionally a domain. Ownership and lineage
 * are not derived from the write; they come from other DataHub ingestion or explicit emitters.
 *
 * Prerequisites
 * - DataHub running locally: pip install acryl-datahub && datahub docker quickstart
 *   (GMS on http://localhost:8080, UI on http://localhost:9002)
 * - The Hudi DataHub sync bundle next to the Spark bundle:
 *     spark-shell --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1,org.apache.hudi:hudi-datahub-sync-bundle:1.2.1 \
 *       <the --conf settings from chapter04/README.md>
 */

import org.apache.spark.sql.SaveMode

val inputPath = "/tmp/input_data"
val basePath  = "file:///tmp/sales_hudi_datahub"
val dbName    = "hudi_db"
val tableName = "sales_hudi"

val raw = spark.read.parquet(inputPath)
val df  = raw.drop(raw.columns.filter(_.startsWith("_hoodie_")): _*)

val writeOpts = Map(
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.recordkey.field" -> "ss_item_sk,ss_ticket_number",
  "hoodie.datasource.write.operation" -> "bulk_insert",
  // DataHub sync after each commit
  "hoodie.datasource.meta.sync.enable" -> "true",
  "hoodie.meta.sync.client.tool.class" -> "org.apache.hudi.sync.datahub.DataHubSyncTool",
  "hoodie.meta.sync.datahub.emitter.server" -> "http://localhost:8080",
  "hoodie.datasource.hive_sync.database" -> dbName,
  "hoodie.datasource.hive_sync.table" -> tableName)

df.write.format("hudi").options(writeOpts).mode(SaveMode.Overwrite).save(basePath)

// Then open http://localhost:9002 and search for "sales_hudi".
