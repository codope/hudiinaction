/*
 * 03_bigquery_sync.scala — Sync a Hudi table to BigQuery (external table via manifest)
 *
 * Prereqs
 * - GCP project + GCS bucket (e.g., gs://my-gcs-bucket/sales_hudi_bq)
 * - Service account with access to GCS + BigQuery (set GOOGLE_APPLICATION_CREDENTIALS)
 * - Include hudi-gcp bundle and spark-bigquery connector when using CLI sync.
 * Docs: Hudi BigQuery — https://hudi.apache.org/docs/gcp_bigquery/
 */

/// spark-shell launcher (Dataproc or local with GCS connector):
// spark-shell --packages \
//  org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
//  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
//  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
//  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
//  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar

val inputPath = "/tmp/input_data/"
val basePath  = "gs://my-gcs-bucket/sales_hudi_bq"
val projectId = "my-gcp-project"
val dataset   = "hudi_bigquery_db"
val tableName = "sales_hudi"

// Prepare a hive-style PARTITIONED DataFrame (partition by ss_store_sk just for demo)
import org.apache.spark.sql.functions._
val raw = spark.read.format("parquet").load(inputPath).drop("_hoodie_commit_time","_hoodie_commit_seqno","_hoodie_record_key","_hoodie_partition_path","_hoodie_file_name")
val df  = raw.withColumn("ss_store_sk_part", coalesce(col("ss_store_sk"), lit(0)))  // safe partition key

import org.apache.spark.sql.SaveMode
val writeOpts = Map(
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
  "hoodie.datasource.write.recordkey.field" -> "ss_item_sk",
  "hoodie.datasource.write.partitionpath.field" -> "ss_store_sk_part",
  "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.SimpleKeyGenerator",
  "hoodie.datasource.write.operation" -> "bulk_insert",
  "hoodie.bulkinsert.shuffle.parallelism" -> "50",

  // Enable BigQuery sync via meta sync classes
  "hoodie.datasource.meta.sync.enable" -> "true",
  "hoodie.datasource.meta.sync.classes" -> "org.apache.hudi.gcp.bigquery.BigQuerySyncTool",
  "hoodie.gcp.project.id" -> projectId,
  "hoodie.gcp.bigquery.sync.dataset.name" -> dataset,
  "hoodie.gcp.bigquery.sync.table.name" -> tableName,
  "hoodie.gcp.bigquery.sync.partition_fields" -> "ss_store_sk_part",
  // Recommend using BigQuery manifest flow
  "hoodie.gcp.bigquery.sync.use_bq_manifest_file" -> "true",
  // (Optional) Use GCS listing for partitioned tables
  "hoodie.gcp.bigquery.sync.use_file_listing_from_gcs" -> "true"
)

df.write.format("hudi").options(writeOpts).mode(SaveMode.Overwrite).save(basePath)

/*
----- Independent sync (CLI) -----
# Using the Hudi sync tool (requires hudi-gcp-bundle):
java -cp /path/to/hudi-gcp-bundle_2.12-1.0.2.jar:/path/to/spark-bigquery-connector.jar \
  org.apache.hudi.gcp.bigquery.BigQuerySyncTool \
  --project-id my-gcp-project \
  --dataset-name hudi_bigquery_db \
  --dataset-location US \
  --table sales_hudi \
  --source-uri gs://my-gcs-bucket/sales_hudi_bq/ \
  --base-path gs://my-gcs-bucket/sales_hudi_bq \
  --partitioned-by ss_store_sk_part \
  --use-bq-manifest-file
*/
// After sync: query from BigQuery as an external table (snapshot on CoW/RO MOR).

// READ (BigQuery):
/*
Option A — bq CLI:
  bq query --use_legacy_sql=false \
    "SELECT COUNT(*) AS rows FROM `${projectId}.${dataset}.${tableName}`"

Option B — Spark (BigQuery connector):
  Launch spark-shell with:
   --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.3.jar
  Then:
   val bq = spark.read.format("bigquery")
     .option("table", s"$projectId.$dataset.$tableName").load()
   bq.selectExpr("count(*) as rows").show(false)
*/
