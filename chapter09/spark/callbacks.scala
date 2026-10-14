// Chapter 9, "Notifying downstream systems after a commit" - a post-commit HTTP callback
// to a local receiver (scripts/http_callback_receiver.py), capturing the real payload.
//
// Kafka and Pulsar callbacks are config-only here, not run (matching the task scope):
//   Kafka:  hoodie.write.commit.callback.kafka.bootstrap.servers / .topic / .partition /
//           .acks / .retries, with hoodie.write.commit.callback.class set to
//           org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback
//   Pulsar: hoodie.write.commit.callback.pulsar.broker.service.url / .topic / producer
//           tuning knobs, with hoodie.write.commit.callback.class set to
//           org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallback

import java.io.File
import org.apache.spark.sql.SaveMode
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/callbacks")
val tablePath = s"$basePath/rides"
val callbackPort = sys.env.getOrElse("CALLBACK_PORT", "9192")
val callbackUrl = s"http://127.0.0.1:$callbackPort/hudi-commits"

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))

val rows = Seq(("r1", "NYC", 12.0), ("r2", "SF", 22.0)).toDF("id", "city", "fare")

println(s"=== Writing with an HTTP post-commit callback to $callbackUrl ===")
rows.write.format("hudi")
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.table.ordering.fields", "id")
  .option("hoodie.table.name", "rides")
  .option("hoodie.write.commit.callback.on", "true")
  .option("hoodie.write.commit.callback.class", "org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback")
  .option("hoodie.write.commit.callback.http.url", callbackUrl)
  .option("hoodie.write.commit.callback.http.api.key", "ch9-demo-key")
  .option("hoodie.write.commit.callback.http.timeout.seconds", "10")
  .mode(SaveMode.Overwrite)
  .save(tablePath)

println("Write complete. See the receiver's stdout (captured to the log by run_callbacks.sh)")
println("for the exact HoodieWriteCommitCallbackMessage JSON payload it received.")

System.exit(0)
