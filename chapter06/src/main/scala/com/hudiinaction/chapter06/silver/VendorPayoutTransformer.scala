package com.hudiinaction.chapter06.silver

import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.utilities.transform.Transformer
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.api.java.JavaSparkContext

class VendorPayoutTransformer extends Transformer with Serializable {

  override def apply(
                      jsc: JavaSparkContext,
      spark: SparkSession,
      inputDf: Dataset[Row],
      properties: TypedProperties
  ): Dataset[Row] = {

    import spark.implicits._

    val withTs = inputDf
      //.withColumn("event_ts", $"event_ts".cast("timestamp"))
      .withColumn("payout_date", date_format($"event_ts", "yyyy-MM-dd"))

    withTs
      .groupBy(
        $"vendor_id",
        $"payout_date",
        $"order_id",
        $"order_line_id",
        $"customer_id",
        $"country",
        $"currency"
      )
      .agg(
        sum(when($"event_type" === "PAYMENT_CAPTURED", $"amount").otherwise(0.0))
          .as("gross_amount"),
        sum(when($"event_type" === "REFUNDED", $"amount").otherwise(0.0))
          .as("refund_amount"),
        sum(when($"event_type" === "FEE_ADJUSTMENT", $"amount").otherwise(0.0))
          .as("fee_amount"),
        sum(when($"event_type" === "CHARGEBACK", $"amount").otherwise(0.0))
          .as("chargeback_amount"),
        min($"event_ts").as("first_event_ts"),
        max($"event_ts").as("last_event_ts")
      )
      .withColumn(
        "net_payout_amount",
        col("gross_amount") -
          col("refund_amount") -
          col("fee_amount") -
          col("chargeback_amount")
      )
      .withColumn("last_updated_ts", current_timestamp())
      .withColumn("last_event_type", lit("AGGREGATED"))
  }
}
