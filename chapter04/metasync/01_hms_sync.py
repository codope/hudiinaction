"""
01_hms_sync.py: sync a Hudi table to a Hive Metastore (HMS). PySpark version of 01_hms_sync.scala.

Dataset: chapter04/input_data (TPC-DS store_sales, not partitioned).
Record key: (ss_item_sk, ss_ticket_number).
Hudi 1.2.1, Spark 3.5. Docs: https://hudi.apache.org/docs/syncing_metastore/

Prerequisite: a Hive Metastore listening on thrift://localhost:9083 (local setup below).

Run with spark-submit, using the options from chapter04/README.md:
  spark-submit <options> /path/to/hudiinaction/chapter04/metasync/01_hms_sync.py
"""

# For a local test, start a standalone metastore from the Hive 2.3.9 jars that ship with Spark.
# Save this as hive-site.xml in a new directory and run the java command from that directory:
#
#   <configuration>
#     <property><name>javax.jdo.option.ConnectionURL</name>
#       <value>jdbc:derby:;databaseName=metastore_db;create=true</value></property>
#     <property><name>datanucleus.schema.autoCreateAll</name><value>true</value></property>
#     <property><name>hive.metastore.schema.verification</name><value>false</value></property>
#     <property><name>hive.async.log.enabled</name><value>false</value></property>
#   </configuration>
#
#   java -cp ".:$SPARK_HOME/jars/*" org.apache.hadoop.hive.metastore.HiveMetaStore -p 9083

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

input_path = "/tmp/input_data"
base_path = "file:///tmp/sales_hudi_hms"
db_name = "default"
table_name = "sales_hudi"

raw = spark.read.parquet(input_path)
df = raw.drop(*[c for c in raw.columns if c.startswith("_hoodie_")])

# ---------- Write-time sync ----------
write_opts = {
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.recordkey.field": "ss_item_sk,ss_ticket_number",
    "hoodie.datasource.write.operation": "bulk_insert",
    # Run the meta sync after each successful commit
    "hoodie.datasource.meta.sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://localhost:9083",
    "hoodie.datasource.hive_sync.database": db_name,
    "hoodie.datasource.hive_sync.table": table_name,
}

df.write.format("hudi").options(**write_opts).mode("overwrite").save(base_path)

# ---------- Independent sync (no write) ----------
# HiveSyncTool is a JVM command-line tool, the same whichever language wrote the table.
# It ships inside the Hudi Spark bundle, so spark-submit can run it with the Hive client jars
# that come with Spark:
#
# curl -fLO https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.2.1/hudi-spark3.5-bundle_2.12-1.2.1.jar
# spark-submit --class org.apache.hudi.hive.HiveSyncTool \
#   hudi-spark3.5-bundle_2.12-1.2.1.jar \
#   --metastore-uris thrift://localhost:9083 \
#   --database default \
#   --table sales_hudi \
#   --base-path file:///tmp/sales_hudi_hms \
#   --sync-mode hms

# ---------- Read through the metastore ----------
# Restart with two extra settings so Spark uses the metastore as its catalog:
#   --conf spark.sql.catalogImplementation=hive \
#   --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083
# Then:
#   spark.sql("SHOW TABLES IN default").show(truncate=False)
#   spark.sql("SELECT count(*) AS rows FROM default.sales_hudi").show(truncate=False)   # 2880404
