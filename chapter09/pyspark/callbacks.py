# Chapter 9, "Notifying downstream systems after a commit" - a post-commit HTTP callback
# to a local receiver (scripts/http_callback_receiver.py), capturing the real payload.
# PySpark version of spark/callbacks.scala. `scripts/run_pyspark.sh callbacks` starts the
# receiver, runs this script and prints what the receiver captured.
#
# Kafka and Pulsar callbacks are config-only here, not run:
#   Kafka:  hoodie.write.commit.callback.kafka.bootstrap.servers / .topic / .partition /
#           .acks / .retries, with hoodie.write.commit.callback.class set to
#           org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback
#   Pulsar: hoodie.write.commit.callback.pulsar.broker.service.url / .topic / producer
#           tuning knobs, with hoodie.write.commit.callback.class set to
#           org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallback

import os
import shutil

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/callbacks")
table_path = f"{base_path}/rides"
callback_port = os.environ.get("CALLBACK_PORT", "9192")
callback_url = f"http://127.0.0.1:{callback_port}/hudi-commits"

shutil.rmtree(base_path, ignore_errors=True)

rows = spark.createDataFrame([("r1", "NYC", 12.0), ("r2", "SF", 22.0)], "id string, city string, fare double")

print(f"=== Writing with an HTTP post-commit callback to {callback_url} ===")
(rows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "bulk_insert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", "rides")
    .option("hoodie.write.commit.callback.on", "true")
    .option("hoodie.write.commit.callback.class", "org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback")
    .option("hoodie.write.commit.callback.http.url", callback_url)
    .option("hoodie.write.commit.callback.http.api.key", "ch9-demo-key")
    .option("hoodie.write.commit.callback.http.timeout.seconds", "10")
    .mode("overwrite")
    .save(table_path))

print("Write complete. See the receiver's output for the exact")
print("HoodieWriteCommitCallbackMessage JSON payload it received.")
