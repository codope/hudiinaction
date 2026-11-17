"""
Kafka -> raw Hudi table (Copy-on-Write), with Spark Structured Streaming.
PySpark version of src/main/scala/.../raw/RawIngestion.scala.

  ./scripts/run_raw_ingestion_pyspark.sh
  # or: spark-submit <options> pyspark/raw_ingestion.py [bootstrap] [topic] [target_path] [checkpoint]
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, from_json, lit
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

spark = SparkSession.builder.appName("Chapter06RawIngestion").getOrCreate()

args = sys.argv[1:]
kafka_bootstrap = args[0] if len(args) > 0 else "localhost:9092"
topic = args[1] if len(args) > 1 else "payments.events"
target_path = args[2] if len(args) > 2 else "/tmp/hudi/raw/payments"
checkpoint_path = args[3] if len(args) > 3 else "/tmp/hudi/checkpoints/raw_ingest"

schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", LongType()),
    StructField("ingest_ts", LongType()),
    StructField("order_id", StringType()),
    StructField("order_line_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("vendor_id", StringType()),
    StructField("currency", StringType()),
    StructField("amount", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("country", StringType()),
    StructField("attributes", StringType()),
    StructField("dt", StringType()),
    StructField("line_version", LongType()),
    StructField("line_opened_ts", LongType()),
    StructField("gross_to_date", DoubleType()),
    StructField("refund_to_date", DoubleType()),
    StructField("fee_to_date", DoubleType()),
    StructField("chargeback_to_date", DoubleType()),
])

raw_kafka = (spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", kafka_bootstrap)
             .option("subscribe", topic)
             .option("startingOffsets", "earliest")
             .load())

value_df = raw_kafka.selectExpr("CAST(value AS STRING) AS json_str")

parsed = (value_df
          .select(from_json(col("json_str"), schema).alias("p"))
          .select("p.*")
          .withColumn("event_ts", (col("event_ts") / lit(1000000)).cast("timestamp"))
          .withColumn("line_opened_ts", (col("line_opened_ts") / lit(1000000)).cast("timestamp"))
          .withColumn("ingest_ts", current_timestamp())
          .withColumn("dt", date_format(col("event_ts"), "yyyy-MM-dd")))

hudi_options = {
    "hoodie.table.name": "hudi_payments_raw",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "event_id",
    "hoodie.datasource.write.precombine.field": "event_ts",
    "hoodie.datasource.write.partitionpath.field": "dt",
    "hoodie.clustering.inline": "true",
    "hoodie.clustering.inline.max.commits": "5",
    "hoodie.clustering.sort.columns": "vendor_id,event_ts",
    "hoodie.metadata.enable": "true",
}

query = (parsed.writeStream
         .format("hudi")
         .options(**hudi_options)
         .option("checkpointLocation", checkpoint_path)
         .outputMode("append")
         .trigger(processingTime="5 minutes")
         .start(target_path))

query.awaitTermination()
