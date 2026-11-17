"""
02_glue_sync.py: sync a Hudi table to the AWS Glue Data Catalog and query it with Athena.
PySpark version of 02_glue_sync.scala.

Hudi 1.2.1, Spark 3.5. Docs: https://hudi.apache.org/docs/syncing_aws_glue_data_catalog/
Not run by the authors (needs an AWS account). Config keys checked against the 1.2.1 source.

Prerequisites
- AWS credentials with Glue and S3 access (instance profile, environment, or credentials file).
- An S3 bucket, and Spark configured for s3a:// paths (hadoop-aws on the classpath).
- The Hudi AWS bundle next to the Spark bundle:
    spark-submit --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1,org.apache.hudi:hudi-aws-bundle:1.2.1 \
      <the --conf settings from chapter04/README.md> 02_glue_sync.py

Athena reads Hudi tables written by Hudi 0.14 and 0.15 (table version 6). To keep a table
written by Hudi 1.2.1 readable by Athena, write it with table version 6 (set below).
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

input_path = "/tmp/input_data"
base_path = "s3a://my-bucket/sales_hudi_glue"
db_name = "hudi_glue_db"
table_name = "sales_hudi"

raw = spark.read.parquet(input_path)
df = raw.drop(*[c for c in raw.columns if c.startswith("_hoodie_")])

write_opts = {
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.recordkey.field": "ss_item_sk,ss_ticket_number",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.write.table.version": "6",   # for Athena; omit if only Hudi 1.x readers use the table
    # Glue sync after each commit
    "hoodie.datasource.meta.sync.enable": "true",
    "hoodie.meta.sync.client.tool.class": "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool",
    "hoodie.datasource.hive_sync.database": db_name,
    "hoodie.datasource.hive_sync.table": table_name,
}

df.write.format("hudi").options(**write_opts).mode("overwrite").save(base_path)

# ---------- Independent sync ----------
# spark-submit --class org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool \
#   --jars hudi-spark3.5-bundle_2.12-1.2.1.jar \
#   hudi-aws-bundle-1.2.1.jar \
#   --base-path s3a://my-bucket/sales_hudi_glue \
#   --database hudi_glue_db \
#   --table sales_hudi
#
# ---------- Query with Athena ----------
# aws athena start-query-execution \
#   --query-string "SELECT count(*) FROM hudi_glue_db.sales_hudi" \
#   --work-group primary \
#   --result-configuration OutputLocation=s3://my-bucket/athena-output/
