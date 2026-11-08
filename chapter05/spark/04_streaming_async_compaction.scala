// Chapter 5, "Configuring async compaction with Spark Structured Streaming" - streaming upserts
// into a Merge-on-Read table, with compaction scheduled every 10 delta commits and executed
// in the background by the same Spark application.
//
// A rate source produces 20 rows a second over 500 keys, so most rows are updates. Each
// 10-second micro-batch is one delta commit. The query runs for about three minutes.

import java.io.File
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter05/streaming_compaction")
val tablePath = s"$basePath/events_stream"
val ckptPath = s"$basePath/checkpoint"
val runSeconds = sys.env.getOrElse("RUN_SECONDS", "180").toInt

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))

val streamingDF = spark.readStream.format("rate").option("rowsPerSecond", 20).load().select(
    concat(lit("e"), col("value") % 500).as("id"),
    (col("value") % 50).as("user_id"),
    col("timestamp").as("event_time"),
    // A key always maps to the same partition, so updates find it.
    when(col("value") % 2 === 0, "2026-01-01").otherwise("2026-01-02").as("datestr"),
    col("value").as("ts"))

val query = {
  streamingDF.writeStream
    .format("org.apache.hudi")
    .option("hoodie.table.name", "events_stream")
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
    .option("hoodie.datasource.write.operation", "upsert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.datasource.write.partitionpath.field", "datestr")
    .option("hoodie.table.ordering.fields", "ts")
    .option("checkpointLocation", ckptPath)
    // Async compaction alongside ingestion
    .option("hoodie.datasource.compaction.async.enable", "true")
    // Periodically schedule by commits
    .option("hoodie.compact.inline.max.delta.commits", "10")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start(tablePath)
}

query.awaitTermination(runSeconds * 1000L)
// Let the running micro-batch commit first, so stop() does not interrupt a write.
while (query.status.isTriggerActive) Thread.sleep(100)
query.stop()

val mc = HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf()).setBasePath(tablePath).build()
val completed = mc.getActiveTimeline.filterCompletedInstants().getInstants.asScala.map(_.getAction)
println(s"\nAfter ${runSeconds}s: ${completed.count(_ == "deltacommit")} delta commits, " +
  s"${completed.count(_ == "commit")} completed compaction(s), " +
  s"${mc.getActiveTimeline.filterPendingCompactionTimeline().countInstants()} pending")
println(s"Rows in the table: ${spark.read.format("hudi").load(tablePath).count()} (500 keys)")

System.exit(0)
