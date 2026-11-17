"""
Chapter 4: creating the store_sales Hudi table and reading it with the Spark DataSource API
and Spark Structured Streaming. PySpark version of hudi_querying_tutorial.scala.

Dataset: TPC-DS store_sales sample (2,880,404 rows) in chapter04/input_data.
Record key: (ss_item_sk, ss_ticket_number), the store_sales primary key.

Run with spark-submit, using the options from README.md:
  spark-submit <options> /path/to/hudiinaction/chapter04/hudi_querying_tutorial.py
or from the pyspark shell:
  >>> exec(open("/path/to/hudiinaction/chapter04/hudi_querying_tutorial.py").read())
"""

from datetime import datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Path to chapter04/input_data in your clone of the repository.
input_path = "/tmp/input_data"
# Where the Hudi table is created. Must start empty.
base_path = "/tmp/hudi_demo_tbl"

# ============================================================================
# SECTION 1: Create the Hudi table with bulk_insert
# ============================================================================

# The sample Parquet files were exported from an older Hudi table and still carry
# _hoodie_* metadata columns, so drop them before writing.
raw = spark.read.parquet(input_path)
df = raw.drop(*[c for c in raw.columns if c.startswith("_hoodie_")])

# No single store_sales column is unique: ss_item_sk has only 18,000 distinct values.
# The primary key is (ss_item_sk, ss_ticket_number), so both fields form the record key.
write_opts = {
    "hoodie.table.name": "hudi_demo_tbl",
    "hoodie.datasource.write.recordkey.field": "ss_item_sk,ss_ticket_number",
    "hoodie.datasource.write.operation": "bulk_insert",
}

df.write.format("hudi").options(**write_opts).mode("overwrite").save(base_path)

# Hudi stores a composite key as field:value pairs, for example
# ss_item_sk:406,ss_ticket_number:188525
spark.read.format("hudi").load(base_path).select("_hoodie_record_key").show(1, truncate=False)

# ============================================================================
# SECTION 2: Query through the Spark DataSource API
# ============================================================================

spark.read.format("hudi").load(base_path).createOrReplaceTempView("hudi_tbl")
spark.sql("select count(*) from hudi_tbl").show(truncate=False)
# +--------+
# |count(1)|
# +--------+
# |2880404 |
# +--------+

spark.sql("select count(*) from hudi_tbl where ss_store_sk = 7").show(truncate=False)
# +--------+
# |count(1)|
# +--------+
# |458194  |
# +--------+

# Open the Spark UI (http://localhost:4040), SQL / DataFrame tab, to see the scan statistics.

# ============================================================================
# SECTION 3: Query through Spark Structured Streaming
# ============================================================================

source_base_path = "/tmp/hudi_demo_tbl"
# With no start offset set, the stream starts from the earliest commit on the timeline
# (hoodie.datasource.streaming.startOffset defaults to earliest), so batch 0 is the whole table.
stream_df = spark.readStream.format("hudi").load(source_base_path)


def consume_batch(batch_df, batch_id):
    batch_df.persist()
    print(f"{datetime.now().isoformat()} start consuming batch {batch_id}")
    print(f"Total record count {batch_df.count()}")
    batch_df.unpersist()
    print(f"{datetime.now().isoformat()} finish")


query = (stream_df.writeStream
         .foreachBatch(consume_batch)
         .option("checkpointLocation", "/tmp/hudi_streaming_hudi/checkpoint/")
         .trigger(processingTime="2 minutes")
         .start())

# Expected output for the first micro-batch:
#   2026-11-17T11:28:45.614263 start consuming batch 0
#   Total record count 2880404
#   2026-11-17T11:28:50.296759 finish
#
# The checkpoint directory records which commits the query has consumed; restarting with the
# same directory resumes after the last processed commit.
#
# In the pyspark shell you can leave the query running and stop it later with query.stop().
# Under spark-submit, wait for the first micro-batch to finish and stop:
query.processAllAvailable()
query.stop()
