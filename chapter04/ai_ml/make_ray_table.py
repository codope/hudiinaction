"""
Writes a small Copy-on-Write copy of store_sales that Ray Data can read, then upserts 20 rows
so the incremental example has a second commit. PySpark version of make_ray_table.scala.

Ray 2.59's read_hudi uses hudi-rs 0.4, which reads table version 6 only, and which finds no
files in a non-partitioned table written by Hudi 1.2.1. So this copy uses table version 6 and
is partitioned by ss_store_sk.

  spark-submit <options from chapter04/README.md> chapter04/ai_ml/make_ray_table.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

spark = SparkSession.builder.getOrCreate()

path = "/tmp/ss_cow_v6_part"
opts = {
    "hoodie.table.name": "ss_cow_v6_part",
    "hoodie.datasource.write.recordkey.field": "ss_item_sk,ss_ticket_number",
    "hoodie.datasource.write.partitionpath.field": "ss_store_sk",
    "hoodie.write.table.version": "6",
}

raw = spark.read.parquet("/tmp/input_data")
small = (raw.drop(*[c for c in raw.columns if c.startswith("_hoodie_")])
         .where("ss_store_sk in (1, 7)").limit(10000))
(small.write.format("hudi").options(**{**opts, "hoodie.datasource.write.operation": "bulk_insert"})
   .mode("overwrite").save(path))

upd = (spark.read.format("hudi").load(path)
       .orderBy(rand()).limit(20)
       .withColumn("ss_quantity", col("ss_quantity") + 1))
upd.write.format("hudi").options(**{**opts, "hoodie.datasource.write.operation": "upsert"}).mode("append").save(path)

# The two commit times, for the incremental read in ray_data_example.py
for name in sorted(f for f in os.listdir(path + "/.hoodie") if f.endswith(".commit")):
    print(name)
