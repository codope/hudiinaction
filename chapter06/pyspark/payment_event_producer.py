"""
Kafka producer for the payment events. Python version of
src/main/scala/.../producer/PaymentEventProducer.scala.

This one is plain Python, not PySpark: it needs only the kafka-python package
(pip install -r pyspark/requirements.txt), so a Python-only setup does not need sbt.

  ./scripts/run_producer_python.sh normal 2000
  ./scripts/run_producer_python.sh polluted 30
  ./scripts/run_producer_python.sh polluted 30 fixed
  # or: python3 pyspark/payment_event_producer.py [bootstrap] [topic] [mode] [iterations] [fixed]
"""

import json
import math
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timezone

from kafka import KafkaProducer


def now_micros():
    return int(time.time() * 1000) * 1000


def stamp(event, micros):
    """Sets the event time (and the matching date) to when the event is sent."""
    return {**event,
            "event_ts": micros,
            "ingest_ts": micros,
            "dt": datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc).strftime("%Y-%m-%d")}


def line_events(n, opened_ts):
    """
    All events for order line n, in version order, with running totals. Every line is
    captured, then charged a fee and partially refunded; every fifth line is also charged back.
    """
    gross = float(10 + (n % 200))
    fee = round(gross * 5) / 100.0
    refund = float(math.floor(gross * 0.3))
    chargeback = gross - refund

    # (event_type, amount, gross, refund, fee, chargeback) after each event
    steps = [
        ("PAYMENT_CAPTURED", gross, gross, 0.0, 0.0, 0.0),
        ("FEE_ADJUSTMENT", fee, gross, 0.0, fee, 0.0),
        ("REFUNDED", refund, gross, refund, fee, 0.0),
    ] + ([("CHARGEBACK", chargeback, gross, refund, fee, chargeback)] if n % 5 == 0 else [])

    return [stamp({
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": opened_ts,
        "ingest_ts": opened_ts,
        "order_id": f"O-{1000 + (n % 100)}",
        "order_line_id": f"OL-{10000 + n}",
        "customer_id": f"C-{500 + (n % 50)}",
        "vendor_id": f"V-{100 + (n % 20)}",
        "currency": "USD",
        "amount": amount,
        "payment_method": "CARD" if n % 2 == 0 else "WALLET",
        "country": "US" if n % 3 == 0 else "GB",
        "attributes": json.dumps({"source": "chapter06", "line": n}, separators=(",", ":")),
        "dt": "",
        # Order line state after this event, as kept by the payment system.
        "line_version": idx + 1,
        "line_opened_ts": opened_ts,
        "gross_to_date": g,
        "refund_to_date": r,
        "fee_to_date": f,
        "chargeback_to_date": c,
    }, opened_ts) for idx, (event_type, amount, g, r, f, c) in enumerate(steps)]


def send(producer, topic, event):
    producer.send(topic, key=event["vendor_id"], value=json.dumps(event, separators=(",", ":")))


def produce_normal(producer, topic, iterations):
    # Odd iterations open a new order line; even ones send the next pending refund, fee or
    # chargeback for an earlier line, so follow-up events arrive well after the capture.
    pending = deque()
    line = 0
    for i in range(1, iterations + 1):
        if i % 2 == 1 or not pending:
            line += 1
            versions = line_events(line, now_micros())
            pending.extend(versions[1:])
            event = versions[0]
        else:
            event = stamp(pending.popleft(), now_micros())
        send(producer, topic, event)
        if i % 100 == 0:
            print(f"[normal] Produced {i} events")
        time.sleep(0.05)


def produce_polluted(producer, topic, lines, fixed):
    """
    A payment-service release that sends one more event (a fee adjustment) for order lines
    1..lines. The events carry valid event IDs and a higher line_version, but the buggy
    release populates gross_to_date ten times too high for every third line. With fixed = True,
    the same events are sent with correct values, as the replay after a restore.
    """
    label = "polluted-fixed" if fixed else "polluted"
    for n in range(1, lines + 1):
        # The line's latest state; its versions are always higher than anything sent before.
        last = line_events(n, now_micros())[-1]
        fee = 1.0
        gross = last["gross_to_date"] * 10 if (not fixed and n % 3 == 0) else last["gross_to_date"]
        event = stamp({
            **last,
            "event_id": str(uuid.uuid4()),
            "event_type": "FEE_ADJUSTMENT",
            "amount": fee,
            "attributes": json.dumps({"source": "chapter06", "line": n, "mode": label}, separators=(",", ":")),
            "line_version": last["line_version"] + 1,
            "gross_to_date": gross,
            "fee_to_date": last["fee_to_date"] + fee,
        }, now_micros())
        send(producer, topic, event)
        if n % 100 == 0:
            print(f"[{label}] Produced {n} events")
        time.sleep(0.02)


def main():
    args = sys.argv[1:]
    bootstrap_servers = args[0] if len(args) > 0 else "localhost:9092"
    topic = args[1] if len(args) > 1 else "payments.events"
    mode = args[2] if len(args) > 2 else "normal"
    iterations = int(args[3]) if len(args) > 3 else 1000
    fixed = len(args) > 4 and args[4] == "fixed"

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, acks=1,
                             key_serializer=str.encode, value_serializer=str.encode)
    try:
        if mode == "normal":
            produce_normal(producer, topic, iterations)
        elif mode == "polluted":
            produce_polluted(producer, topic, iterations, fixed)
        else:
            raise ValueError(f"Unsupported mode: {mode}")
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
