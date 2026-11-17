"""
Writes a small store_sales Copy-on-Write table with Hudi 0.15.0, for daft_example.py.
PySpark version of make_daft_table_hudi015.scala.

Run with spark-submit and org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0
(same --conf settings as in chapter04/README.md):
  spark-submit --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 <confs> \
    chapter04/ai_ml/make_daft_table_hudi015.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

raw = spark.read.parquet("/tmp/input_data")
small = (raw.drop(*[c for c in raw.columns if c.startswith("_hoodie_")])
         .where("ss_store_sk in (1, 7)").limit(10000))
(small.write.format("hudi")
   .option("hoodie.table.name", "ss_cow_015")
   .option("hoodie.datasource.write.recordkey.field", "ss_item_sk,ss_ticket_number")
   .option("hoodie.datasource.write.partitionpath.field", "ss_store_sk")
   .option("hoodie.datasource.write.precombine.field", "ss_sold_time_sk")
   .option("hoodie.datasource.write.operation", "bulk_insert")
   .mode("overwrite").save("/tmp/ss_cow_015"))
