/*
 * 02_glue_sync.scala: sync a Hudi table to the AWS Glue Data Catalog and query it with Athena.
 *
 * Hudi 1.2.1, Spark 3.5. Docs: https://hudi.apache.org/docs/syncing_aws_glue_data_catalog/
 * Not run by the authors (needs an AWS account). Config keys checked against the 1.2.1 source.
 *
 * Prerequisites
 * - AWS credentials with Glue and S3 access (instance profile, environment, or credentials file).
 * - An S3 bucket, and Spark configured for s3a:// paths (hadoop-aws on the classpath).
 * - The Hudi AWS bundle next to the Spark bundle:
 *     spark-shell --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1,org.apache.hudi:hudi-aws-bundle:1.2.1 \
 *       <the --conf settings from chapter04/README.md>
 *
 * Athena reads Hudi tables written by Hudi 0.14 and 0.15 (table version 6). To keep a table
 * written by Hudi 1.2.1 readable by Athena, write it with table version 6 (set below).
 */

import org.apache.spark.sql.SaveMode

val inputPath = "/tmp/input_data"
val basePath  = "s3a://my-bucket/sales_hudi_glue"
val dbName    = "hudi_glue_db"
val tableName = "sales_hudi"

val raw = spark.read.parquet(inputPath)
val df  = raw.drop(raw.columns.filter(_.startsWith("_hoodie_")): _*)

val writeOpts = Map(
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.recordkey.field" -> "ss_item_sk,ss_ticket_number",
  "hoodie.datasource.write.operation" -> "bulk_insert",
  "hoodie.write.table.version" -> "6",   // for Athena; omit if only Hudi 1.x readers use the table
  // Glue sync after each commit
  "hoodie.datasource.meta.sync.enable" -> "true",
  "hoodie.meta.sync.client.tool.class" -> "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool",
  "hoodie.datasource.hive_sync.database" -> dbName,
  "hoodie.datasource.hive_sync.table" -> tableName)

df.write.format("hudi").options(writeOpts).mode(SaveMode.Overwrite).save(basePath)

/*
---------- Independent sync ----------
spark-submit --class org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool \
  --jars hudi-spark3.5-bundle_2.12-1.2.1.jar \
  hudi-aws-bundle-1.2.1.jar \
  --base-path s3a://my-bucket/sales_hudi_glue \
  --database hudi_glue_db \
  --table sales_hudi

---------- Query with Athena ----------
aws athena start-query-execution \
  --query-string "SELECT count(*) FROM hudi_glue_db.sales_hudi" \
  --work-group primary \
  --result-configuration OutputLocation=s3://my-bucket/athena-output/
*/
