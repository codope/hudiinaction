package com.hudiinaction.chapter06.governance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TimeTravelExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Chapter06TimeTravelExamples")
      .getOrCreate()

    import spark.implicits._

    val tableType = if (args.nonEmpty) args(0) else "raw"              // raw | silver
    val instant   = if (args.length > 1) args(1) else ""
    val vendorId  = if (args.length > 2) args(2) else "V-101"

    if (instant.isEmpty) {
      throw new IllegalArgumentException(
        "Usage: TimeTravelExamples <raw|silver> <instant_time> [vendor_id]"
      )
    }

    val path = tableType match {
      case "raw"    => "/tmp/hudi/raw/payments"
      case "silver" => "/tmp/hudi/silver/vendor_payouts"
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported table type '$other'. Expected 'raw' or 'silver'."
        )
    }

    val historicalDf = spark.read.format("hudi")
      .option("as.of.instant", instant)
      .load(path)

    val currentDf = spark.read.format("hudi")
      .load(path)

    val historicalCount = historicalDf.filter($"vendor_id" === vendorId).count()
    val currentCount = currentDf.filter($"vendor_id" === vendorId).count()

    println("==================================================")
    println(s"Table type         : $tableType")
    println(s"Path               : $path")
    println(s"Vendor             : $vendorId")
    println(s"Historical instant : $instant")
    println(s"Historical count   : $historicalCount")
    println(s"Current count      : $currentCount")
    println("==================================================")

    println(s"Historical rows for vendor_id=$vendorId")
    historicalDf
      .filter($"vendor_id" === vendorId)
      .orderBy(desc(sortColumn(tableType)))
      .show(50, truncate = false)

    println(s"Current rows for vendor_id=$vendorId")
    currentDf
      .filter($"vendor_id" === vendorId)
      .orderBy(desc(sortColumn(tableType)))
      .show(50, truncate = false)
  }

  private def sortColumn(tableType: String): String =
    tableType match {
      case "raw"    => "event_ts"
      case "silver" => "payout_date"
    }
}