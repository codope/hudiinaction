/*
 * 03_bigquery_sync.scala: sync a Hudi table to Google BigQuery as an external table.
 *
 * Hudi 1.2.1, Spark 3.5. Docs: https://hudi.apache.org/docs/gcp_bigquery/
 * Not run by the authors (needs a GCP project). Config keys checked against the 1.2.1 source.
 *
 * BigQuery sees Copy-on-Write tables and the read-optimized view of Merge-on-Read tables.
 *
 * Prerequisites
 * - A GCP project, a GCS bucket, and credentials with GCS and BigQuery access
 *   (GOOGLE_APPLICATION_CREDENTIALS). Spark configured for gs:// paths (GCS connector).
 * - The Hudi GCP bundle next to the Spark bundle:
 *     spark-shell --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1,org.apache.hudi:hudi-gcp-bundle:1.2.1 \
 *       <the --conf settings from chapter04/README.md>
 */

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

val inputPath = "/tmp/input_data"
val basePath  = "gs://my-gcs-bucket/sales_hudi_bq"
val projectId = "my-gcp-project"
val dataset   = "hudi_bigquery_db"
val tableName = "sales_hudi"

// BigQuery sync expects a hive-style partitioned table; partition by store for the demo.
val raw = spark.read.parquet(inputPath)
val df  = raw.drop(raw.columns.filter(_.startsWith("_hoodie_")): _*)
  .withColumn("store", coalesce(col("ss_store_sk"), lit(0)))

val writeOpts = Map(
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.recordkey.field" -> "ss_item_sk,ss_ticket_number",
  "hoodie.datasource.write.partitionpath.field" -> "store",
  "hoodie.datasource.write.hive_style_partitioning" -> "true",
  "hoodie.datasource.write.operation" -> "bulk_insert",
  // BigQuery sync after each commit
  "hoodie.datasource.meta.sync.enable" -> "true",
  "hoodie.meta.sync.client.tool.class" -> "org.apache.hudi.gcp.bigquery.BigQuerySyncTool",
  "hoodie.gcp.bigquery.sync.project_id" -> projectId,
  "hoodie.gcp.bigquery.sync.dataset_name" -> dataset,
  "hoodie.gcp.bigquery.sync.dataset_location" -> "US",
  "hoodie.gcp.bigquery.sync.table_name" -> tableName,
  "hoodie.gcp.bigquery.sync.source_uri" -> s"$basePath/store=*",
  "hoodie.gcp.bigquery.sync.source_uri_prefix" -> s"$basePath/",
  "hoodie.gcp.bigquery.sync.partition_fields" -> "store",
  "hoodie.gcp.bigquery.sync.use_bq_manifest_file" -> "true")

df.write.format("hudi").options(writeOpts).mode(SaveMode.Overwrite).save(basePath)

/*
---------- Independent sync ----------
spark-submit --class org.apache.hudi.gcp.bigquery.BigQuerySyncTool \
  --jars hudi-spark3.5-bundle_2.12-1.2.1.jar \
  hudi-gcp-bundle-1.2.1.jar \
  --project-id my-gcp-project \
  --dataset-name hudi_bigquery_db \
  --dataset-location US \
  --table sales_hudi \
  --source-uri gs://my-gcs-bucket/sales_hudi_bq/store=* \
  --source-uri-prefix gs://my-gcs-bucket/sales_hudi_bq/ \
  --base-path gs://my-gcs-bucket/sales_hudi_bq \
  --partitioned-by store \
  --use-bq-manifest-file

---------- Query ----------
bq query --use_legacy_sql=false \
  'SELECT count(*) AS rows FROM `my-gcp-project.hudi_bigquery_db.sales_hudi`'
*/
