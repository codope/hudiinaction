"""
03_bigquery_sync.py: sync a Hudi table to Google BigQuery as an external table.
PySpark version of 03_bigquery_sync.scala.

Hudi 1.2.1, Spark 3.5. Docs: https://hudi.apache.org/docs/gcp_bigquery/
Not run by the authors (needs a GCP project). Config keys checked against the 1.2.1 source.

BigQuery sees Copy-on-Write tables and the read-optimized view of Merge-on-Read tables.

Prerequisites
- A GCP project, a GCS bucket, and credentials with GCS and BigQuery access
  (GOOGLE_APPLICATION_CREDENTIALS). Spark configured for gs:// paths (GCS connector).
- The Hudi GCP bundle next to the Spark bundle:
    spark-submit --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1,org.apache.hudi:hudi-gcp-bundle:1.2.1 \
      <the --conf settings from chapter04/README.md> 03_bigquery_sync.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit

spark = SparkSession.builder.getOrCreate()

input_path = "/tmp/input_data"
base_path = "gs://my-gcs-bucket/sales_hudi_bq"
project_id = "my-gcp-project"
dataset = "hudi_bigquery_db"
table_name = "sales_hudi"

# BigQuery sync expects a hive-style partitioned table; partition by store for the demo.
raw = spark.read.parquet(input_path)
df = (raw.drop(*[c for c in raw.columns if c.startswith("_hoodie_")])
      .withColumn("store", coalesce(col("ss_store_sk"), lit(0))))

write_opts = {
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.recordkey.field": "ss_item_sk,ss_ticket_number",
    "hoodie.datasource.write.partitionpath.field": "store",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.write.operation": "bulk_insert",
    # BigQuery sync after each commit
    "hoodie.datasource.meta.sync.enable": "true",
    "hoodie.meta.sync.client.tool.class": "org.apache.hudi.gcp.bigquery.BigQuerySyncTool",
    "hoodie.gcp.bigquery.sync.project_id": project_id,
    "hoodie.gcp.bigquery.sync.dataset_name": dataset,
    "hoodie.gcp.bigquery.sync.dataset_location": "US",
    "hoodie.gcp.bigquery.sync.table_name": table_name,
    "hoodie.gcp.bigquery.sync.source_uri": f"{base_path}/store=*",
    "hoodie.gcp.bigquery.sync.source_uri_prefix": f"{base_path}/",
    "hoodie.gcp.bigquery.sync.partition_fields": "store",
    "hoodie.gcp.bigquery.sync.use_bq_manifest_file": "true",
}

df.write.format("hudi").options(**write_opts).mode("overwrite").save(base_path)

# ---------- Independent sync ----------
# spark-submit --class org.apache.hudi.gcp.bigquery.BigQuerySyncTool \
#   --jars hudi-spark3.5-bundle_2.12-1.2.1.jar \
#   hudi-gcp-bundle-1.2.1.jar \
#   --project-id my-gcp-project \
#   --dataset-name hudi_bigquery_db \
#   --dataset-location US \
#   --table sales_hudi \
#   --source-uri gs://my-gcs-bucket/sales_hudi_bq/store=* \
#   --source-uri-prefix gs://my-gcs-bucket/sales_hudi_bq/ \
#   --base-path gs://my-gcs-bucket/sales_hudi_bq \
#   --partitioned-by store \
#   --use-bq-manifest-file
#
# ---------- Query ----------
# bq query --use_legacy_sql=false \
#   'SELECT count(*) AS rows FROM `my-gcp-project.hudi_bigquery_db.sales_hudi`'
