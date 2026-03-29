package com.hudiinaction.chapter06.producer

import java.time.Instant
import java.util.Properties
import java.util.UUID

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
                                 dt: String
                               )

  private val EventTypes = Vector(
    "PAYMENT_CAPTURED",
    "REFUNDED",
    "FEE_ADJUSTMENT",
    "CHARGEBACK"
  )

  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.nonEmpty) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "payments.events"
    val mode = if (args.length > 2) args(2) else "normal"
    val iterations = if (args.length > 3) args(3).toInt else 1000

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
        case "retry_storm" =>
          produceRetryStorm(producer, topic, iterations)
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
    (1 to iterations).foreach { i =>
      val event = freshEvent(i)
      producer.send(new ProducerRecord[String, String](topic, event.vendor_id, toJson(event)))
      if (i % 100 == 0) {
        println(s"[normal] Produced $i events")
      }
      Thread.sleep(50)
    }
  }

  private def produceRetryStorm(
                                 producer: KafkaProducer[String, String],
                                 topic: String,
                                 iterations: Int
                               ): Unit = {
    val baselinePool = (1 to 50).map(freshEvent).toVector

    (1 to iterations).foreach { i =>
      val base = baselinePool(i % baselinePool.size)
      val now = Instant.now()
      val newMicros = now.toEpochMilli * 1000 + i

      val updated = base.copy(
        event_ts = newMicros,
        ingest_ts = newMicros,
        attributes = s"""{"mode":"retry_storm","sequence":$i}"""
      )

      producer.send(new ProducerRecord[String, String](topic, updated.vendor_id, toJson(updated)))

      if (i % 100 == 0) {
        println(s"[retry_storm] Produced $i update events")
      }
      Thread.sleep(20)
    }
  }

  private def freshEvent(i: Int): PaymentEvent = {
    val now = Instant.now()
    val micros = now.toEpochMilli * 1000
    val date = now.toString.substring(0, 10)

    PaymentEvent(
      event_id = UUID.randomUUID().toString,
      event_type = EventTypes(i % EventTypes.size),
      event_ts = micros,
      ingest_ts = micros,
      order_id = s"O-${1000 + (i % 100)}",
      order_line_id = s"OL-${10000 + i}",
      customer_id = s"C-${500 + (i % 50)}",
      vendor_id = s"V-${100 + (i % 20)}",
      currency = "USD",
      amount = (10 + (i % 200)).toDouble,
      payment_method = if (i % 2 == 0) "CARD" else "WALLET",
      country = if (i % 3 == 0) "US" else "GB",
      attributes = s"""{"source":"chapter06","sequence":$i}""",
      dt = date
    )
  }

  private def toJson(event: PaymentEvent): String = {
    s"""{"event_id":"${event.event_id}","event_type":"${event.event_type}","event_ts":${event.event_ts},"ingest_ts":${event.ingest_ts},"order_id":"${event.order_id}","order_line_id":"${event.order_line_id}","customer_id":"${event.customer_id}","vendor_id":"${event.vendor_id}","currency":"${event.currency}","amount":${event.amount},"payment_method":"${event.payment_method}","country":"${event.country}","attributes":${quote(event.attributes)},"dt":"${event.dt}"}"""
  }

  private def quote(value: String): String =
    "\"" + value.replace("\"", "\\\"") + "\""
}