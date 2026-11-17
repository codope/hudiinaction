# Chapter 5, "Configuring async compaction with Spark Structured Streaming" - streaming upserts
# into a Merge-on-Read table, with compaction scheduled every 10 delta commits and executed
# in the background by the same Spark application. PySpark version of
# spark/04_streaming_async_compaction.scala.
#
# A rate source produces 20 rows a second over 500 keys, so most rows are updates. Each
# 10-second micro-batch is one delta commit. The query runs for about three minutes.

import os
import shutil
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, when

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter05/streaming_compaction")
table_path = f"{base_path}/events_stream"
ckpt_path = f"{base_path}/checkpoint"
run_seconds = int(os.environ.get("RUN_SECONDS", "180"))
shutil.rmtree(base_path, ignore_errors=True)

streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 20).load().select(
    concat(lit("e"), col("value") % 500).alias("id"),
    (col("value") % 50).alias("user_id"),
    col("timestamp").alias("event_time"),
    # A key always maps to the same partition, so updates find it.
    when(col("value") % 2 == 0, "2026-01-01").otherwise("2026-01-02").alias("datestr"),
    col("value").alias("ts"))

query = (streaming_df.writeStream
         .format("org.apache.hudi")
         .option("hoodie.table.name", "events_stream")
         .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
         .option("hoodie.datasource.write.operation", "upsert")
         .option("hoodie.datasource.write.recordkey.field", "id")
         .option("hoodie.datasource.write.partitionpath.field", "datestr")
         .option("hoodie.table.ordering.fields", "ts")
         .option("checkpointLocation", ckpt_path)
         # Async compaction alongside ingestion
         .option("hoodie.datasource.compaction.async.enable", "true")
         # Periodically schedule by commits
         .option("hoodie.compact.inline.max.delta.commits", "10")
         .outputMode("append")
         .trigger(processingTime="10 seconds")
         .start(table_path))

query.awaitTermination(run_seconds)
# Let the running micro-batch commit first, so stop() does not interrupt a write.
while query.status["isTriggerActive"]:
    time.sleep(0.1)
query.stop()

timeline = spark.sql(f"CALL show_timeline(path => '{table_path}', limit => 1000)").collect()
completed = [r["action"] for r in timeline if r["state"] == "COMPLETED"]
pending = sum(1 for r in timeline if r["action"] == "compaction" and r["state"] != "COMPLETED")
print(f"\nAfter {run_seconds}s: {completed.count('deltacommit')} delta commits, "
      f"{completed.count('commit')} completed compaction(s), {pending} pending")
print(f"Rows in the table: {spark.read.format('hudi').load(table_path).count()} (500 keys)")
