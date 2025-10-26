/*
 * 02_glue_sync.scala — Sync a Hudi table to AWS Glue Data Catalog
 *
 * Prereqs
 * - Running on EMR or Spark with AWS creds (instance profile or env/credentials file).
 * - S3 bucket available (e.g., s3://my-bucket/sales_hudi_glue).
 * - Add Hudi AWS bundle on the classpath when using the CLI sync.
 * Docs: Glue sync — https://hudi.apache.org/docs/syncing_aws_glue_data_catalog/
 */

/// spark-shell launcher (on EMR or local with creds):
// spark-shell --packages \
//  org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
//  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
//  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
//  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
//  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar

val inputPath = "/tmp/input_data/"
val basePath  = "s3a://my-bucket/sales_hudi_glue"
val dbName    = "hudi_glue_db"
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

  // Glue sync (note: mode="glue")
  "hoodie.datasource.meta.sync.enable" -> "true",
  "hoodie.datasource.hive_sync.enable" -> "true",
  "hoodie.datasource.hive_sync.mode" -> "glue",
  "hoodie.datasource.hive_sync.database" -> dbName,
  "hoodie.datasource.hive_sync.table" -> tableName
)

df.write.format("hudi").options(writeOpts).mode(SaveMode.Overwrite).save(basePath)

// Verify via Athena after Glue sync; create db if needed, then query _rt/_ro on MOR.
// Athena notes & support matrix linked in AWS docs.

/*
----- Independent sync (CLI) -----
# Using the Hudi sync tool script:
sh /path/to/run_sync_tool.sh \
  --sync-mode glue \
  --base-path s3://my-bucket/sales_hudi_glue \
  --database hudi_glue_db \
  --table sales_hudi

# Or using spark-submit directly:
spark-submit \
  --class org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2,org.apache.hudi:hudi-aws-bundle_2.12:1.0.2 \
  /path/to/hudi-aws-bundle_2.12-1.0.2.jar \
  --base-path s3://my-bucket/sales_hudi_glue \
  --database hudi_glue_db \
  --table sales_hudi
*/

// READ (Athena):
/*
Option A — AWS CLI (simple & reliable):
  aws athena start-query-execution \
    --query-string "SELECT COUNT(*) AS rows FROM \"hudi_glue_db\".\"sales_hudi\";" \
    --work-group primary \
    --result-configuration OutputLocation=s3://my-bucket/athena-output/

Option B — Scala (AWS SDK v2) if you add the Athena SDK jar at launch:
  --packages software.amazon.awssdk:athena:2.25.48
  Then use StartQueryExecution/GetQueryExecution/GetQueryResults to print results.
*/
