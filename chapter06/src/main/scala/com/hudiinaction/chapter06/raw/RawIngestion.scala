package com.hudiinaction.chapter06.raw

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

object RawIngestion {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Chapter06RawIngestion")
      .getOrCreate()

    import spark.implicits._

    val kafkaBootstrap = if (args.nonEmpty) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "payments.events"
    val targetPath = if (args.length > 2) args(2) else "/tmp/hudi/raw/payments"
    val checkpointPath =
      if (args.length > 3) args(3) else "/tmp/hudi/checkpoints/raw_ingest"

    val schema = new org.apache.spark.sql.types.StructType()
      .add("event_id", "string")
      .add("event_type", "string")
      .add("event_ts", "long")
      .add("ingest_ts", "long")
      .add("order_id", "string")
      .add("order_line_id", "string")
      .add("customer_id", "string")
      .add("vendor_id", "string")
      .add("currency", "string")
      .add("amount", "double")
      .add("payment_method", "string")
      .add("country", "string")
      .add("attributes", "string")
      .add("dt", "string")

    val rawKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    val valueDf = rawKafka.selectExpr("CAST(value AS STRING) AS json_str")

    val parsed = valueDf
      .select(from_json(col("json_str"), schema).alias("p"))
      .select("p.*")
      .withColumn("event_ts", ($"event_ts" / lit(1000000)).cast("timestamp"))
      .withColumn("ingest_ts", current_timestamp())
      .withColumn("dt", date_format(col("event_ts"), "yyyy-MM-dd"))

    val hudiOptions = Map(
      "hoodie.table.name" -> "hudi_payments_raw",
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.recordkey.field" -> "event_id",
      "hoodie.datasource.write.precombine.field" -> "event_ts",
      "hoodie.datasource.write.partitionpath.field" -> "dt",
      "hoodie.clustering.inline" -> "true",
      "hoodie.clustering.inline.max.commits" -> "5",
      "hoodie.clustering.sort.columns" -> "vendor_id,event_ts",
      "hoodie.metadata.enable" -> "true"
    )

    val query = parsed.writeStream
      .format("hudi")
      .options(hudiOptions)
      .option("checkpointLocation", checkpointPath)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 minutes"))
      .start(targetPath)

    query.awaitTermination()
  }
}
