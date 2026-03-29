package com.hudiinaction.chapter06.governance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GdprDelete {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Chapter06GdprDelete")
      .getOrCreate()

    import spark.implicits._

    val vendorId   = if (args.nonEmpty) args(0) else "V-100"
    val rawPath    = if (args.length > 1) args(1) else "/tmp/hudi/raw/payments"
    val silverPath = if (args.length > 2) args(2) else "/tmp/hudi/silver/vendor_payouts"

    println(s"Starting GDPR delete for vendor_id=$vendorId")

    val rawBefore = spark.read.format("hudi").load(rawPath)
      .filter($"vendor_id" === vendorId)
      .count()

    val silverBefore = spark.read.format("hudi").load(silverPath)
      .filter($"vendor_id" === vendorId)
      .count()

    println(s"Rows before delete -> raw=$rawBefore, silver=$silverBefore")

    val rawToDelete = spark.read.format("hudi")
      .load(rawPath)
      .filter($"vendor_id" === vendorId)
      .select("event_id", "dt", "event_ts")

    if (rawToDelete.head(1).nonEmpty) {
      rawToDelete.write
        .format("hudi")
        .option("hoodie.table.name", "hudi_payments_raw")
        .option("hoodie.datasource.write.recordkey.field", "event_id")
        .option("hoodie.datasource.write.precombine.field", "event_ts")
        .option("hoodie.datasource.write.partitionpath.field", "dt")
        .option("hoodie.datasource.write.operation", "delete")
        .mode("append")
        .save(rawPath)
    } else {
      println(s"No raw rows found for vendor_id=$vendorId")
    }

    val silverToDelete = spark.read.format("hudi")
      .load(silverPath)
      .filter($"vendor_id" === vendorId)
      .select("vendor_id", "order_line_id", "payout_date", "last_updated_ts")

    if (silverToDelete.head(1).nonEmpty) {
      silverToDelete.write
        .format("hudi")
        .option("hoodie.table.name", "hudi_vendor_payouts")
        .option("hoodie.datasource.write.recordkey.field", "vendor_id,order_line_id")
        .option("hoodie.datasource.write.precombine.field", "last_updated_ts")
        .option("hoodie.datasource.write.partitionpath.field", "payout_date")
        .option("hoodie.datasource.write.operation", "delete")
        .mode("append")
        .save(silverPath)
    } else {
      println(s"No silver rows found for vendor_id=$vendorId")
    }

    val rawAfter = spark.read.format("hudi").load(rawPath)
      .filter($"vendor_id" === vendorId)
      .count()

    val silverAfter = spark.read.format("hudi").load(silverPath)
      .filter($"vendor_id" === vendorId)
      .count()

    println(s"Rows after delete  -> raw=$rawAfter, silver=$silverAfter")
    println(s"GDPR delete complete for vendor_id=$vendorId")
  }
}
