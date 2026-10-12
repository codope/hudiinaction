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

    // Each event carries its order line's running totals, so the payout record comes from the
    // event alone. When several versions of a line meet (in one batch, or against the stored
    // record), Hudi keeps the highest line_version.
    inputDf.select(
      $"vendor_id",
      // The line's capture date: the same on every event, so the record never changes partition.
      date_format($"line_opened_ts", "yyyy-MM-dd").as("payout_date"),
      $"order_id",
      $"order_line_id",
      $"customer_id",
      $"country",
      $"currency",
      $"gross_to_date".as("gross_amount"),
      $"refund_to_date".as("refund_amount"),
      $"fee_to_date".as("fee_amount"),
      $"chargeback_to_date".as("chargeback_amount"),
      ($"gross_to_date" - $"refund_to_date" - $"fee_to_date" - $"chargeback_to_date")
        .as("net_payout_amount"),
      $"line_opened_ts".as("first_event_ts"),
      $"event_ts".as("last_event_ts"),
      $"line_version",
      current_timestamp().as("last_updated_ts"),
      $"event_type".as("last_event_type")
    )
  }
}
