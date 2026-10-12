package com.hudiinaction.chapter06.producer

import java.time.Instant
import java.util.Properties
import java.util.UUID

import scala.collection.mutable

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object PaymentEventProducer {

  final case class PaymentEvent(
                                 event_id: String,
                                 event_type: String,
                                 event_ts: Long,
                                 ingest_ts: Long,
                                 order_id: String,
                                 order_line_id: String,
                                 customer_id: String,
                                 vendor_id: String,
                                 currency: String,
                                 amount: Double,
                                 payment_method: String,
                                 country: String,
                                 attributes: String,
                                 dt: String,
                                 // Order line state after this event, as kept by the payment system.
                                 line_version: Long,
                                 line_opened_ts: Long,
                                 gross_to_date: Double,
                                 refund_to_date: Double,
                                 fee_to_date: Double,
                                 chargeback_to_date: Double
                               )

  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.nonEmpty) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "payments.events"
    val mode = if (args.length > 2) args(2) else "normal"
    val iterations = if (args.length > 3) args(3).toInt else 1000
    val fixed = args.length > 4 && args(4) == "fixed"

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "1")

    val producer = new KafkaProducer[String, String](props)

    try {
      mode match {
        case "normal" =>
          produceNormal(producer, topic, iterations)
        case "polluted" =>
          producePolluted(producer, topic, iterations, fixed)
        case other =>
          throw new IllegalArgumentException(s"Unsupported mode: $other")
      }
      producer.flush()
    } finally {
      producer.close()
    }
  }

  private def produceNormal(
                             producer: KafkaProducer[String, String],
                             topic: String,
                             iterations: Int
                           ): Unit = {
    // Odd iterations open a new order line; even ones send the next pending refund, fee or
    // chargeback for an earlier line, so follow-up events arrive well after the capture.
    val pending = mutable.Queue.empty[PaymentEvent]
    var line = 0

    (1 to iterations).foreach { i =>
      val event =
        if (i % 2 == 1 || pending.isEmpty) {
          line += 1
          val versions = lineEvents(line, nowMicros())
          pending ++= versions.tail
          versions.head
        } else {
          stamp(pending.dequeue(), nowMicros())
        }
      producer.send(new ProducerRecord[String, String](topic, event.vendor_id, toJson(event)))
      if (i % 100 == 0) {
        println(s"[normal] Produced $i events")
      }
      Thread.sleep(50)
    }
  }

  /**
   * A payment-service release that sends one more event (a fee adjustment) for order lines
   * 1..lines. The events carry valid event IDs and a higher line_version, but the buggy
   * release populates gross_to_date ten times too high for every third line. With fixed = true,
   * the same events are sent with correct values, as the replay after a restore.
   */
  private def producePolluted(
                               producer: KafkaProducer[String, String],
                               topic: String,
                               lines: Int,
                               fixed: Boolean
                             ): Unit = {
    val label = if (fixed) "polluted-fixed" else "polluted"

    (1 to lines).foreach { n =>
      // The line's latest state; its versions are always higher than anything sent before.
      val last = lineEvents(n, nowMicros()).last
      val fee = 1.0
      val gross = if (!fixed && n % 3 == 0) last.gross_to_date * 10 else last.gross_to_date

      val event = stamp(
        last.copy(
          event_id = UUID.randomUUID().toString,
          event_type = "FEE_ADJUSTMENT",
          amount = fee,
          attributes = s"""{"source":"chapter06","line":$n,"mode":"$label"}""",
          line_version = last.line_version + 1,
          gross_to_date = gross,
          fee_to_date = last.fee_to_date + fee
        ),
        nowMicros()
      )
      producer.send(new ProducerRecord[String, String](topic, event.vendor_id, toJson(event)))

      if (n % 100 == 0) {
        println(s"[$label] Produced $n events")
      }
      Thread.sleep(20)
    }
  }


  /**
   * All events for order line n, in version order, with running totals. Every line is
   * captured, then charged a fee and partially refunded; every fifth line is also charged back.
   */
  private def lineEvents(n: Int, openedTs: Long): Seq[PaymentEvent] = {
    val gross = (10 + (n % 200)).toDouble
    val fee = math.round(gross * 5) / 100.0
    val refund = math.floor(gross * 0.3)
    val chargeback = gross - refund

    // (event_type, amount, gross, refund, fee, chargeback) after each event
    val steps = Seq(
      ("PAYMENT_CAPTURED", gross, gross, 0.0, 0.0, 0.0),
      ("FEE_ADJUSTMENT", fee, gross, 0.0, fee, 0.0),
      ("REFUNDED", refund, gross, refund, fee, 0.0)
    ) ++ (if (n % 5 == 0) Seq(("CHARGEBACK", chargeback, gross, refund, fee, chargeback)) else Nil)

    steps.zipWithIndex.map { case ((eventType, amount, g, r, f, c), idx) =>
      stamp(
        PaymentEvent(
          event_id = UUID.randomUUID().toString,
          event_type = eventType,
          event_ts = openedTs,
          ingest_ts = openedTs,
          order_id = s"O-${1000 + (n % 100)}",
          order_line_id = s"OL-${10000 + n}",
          customer_id = s"C-${500 + (n % 50)}",
          vendor_id = s"V-${100 + (n % 20)}",
          currency = "USD",
          amount = amount,
          payment_method = if (n % 2 == 0) "CARD" else "WALLET",
          country = if (n % 3 == 0) "US" else "GB",
          attributes = s"""{"source":"chapter06","line":$n}""",
          dt = "",
          line_version = idx + 1L,
          line_opened_ts = openedTs,
          gross_to_date = g,
          refund_to_date = r,
          fee_to_date = f,
          chargeback_to_date = c
        ),
        openedTs
      )
    }
  }

  /** Sets the event time (and the matching date) to when the event is sent. */
  private def stamp(event: PaymentEvent, micros: Long): PaymentEvent =
    event.copy(
      event_ts = micros,
      ingest_ts = micros,
      dt = Instant.ofEpochMilli(micros / 1000).toString.substring(0, 10)
    )

  private def nowMicros(): Long = Instant.now().toEpochMilli * 1000

  private def toJson(event: PaymentEvent): String = {
    s"""{"event_id":"${event.event_id}","event_type":"${event.event_type}","event_ts":${event.event_ts},"ingest_ts":${event.ingest_ts},"order_id":"${event.order_id}","order_line_id":"${event.order_line_id}","customer_id":"${event.customer_id}","vendor_id":"${event.vendor_id}","currency":"${event.currency}","amount":${event.amount},"payment_method":"${event.payment_method}","country":"${event.country}","attributes":${quote(event.attributes)},"dt":"${event.dt}","line_version":${event.line_version},"line_opened_ts":${event.line_opened_ts},"gross_to_date":${event.gross_to_date},"refund_to_date":${event.refund_to_date},"fee_to_date":${event.fee_to_date},"chargeback_to_date":${event.chargeback_to_date}}"""
  }

  private def quote(value: String): String =
    "\"" + value.replace("\"", "\\\"") + "\""
}