// Chapter 5, "Configuring compaction with Spark datasource writer" - inline compaction on a
// Merge-on-Read table with the NUM_OR_TIME trigger: compact after 10 delta commits or 30
// minutes, whichever comes first.
//
// One insert and ten upserts of the same keys. The upserts go to log files; the 10th delta
// commit triggers compaction inside the write, which shows up on the timeline as a `commit`
// action and as new base files.

import java.io.File
import scala.collection.JavaConverters._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter05/inline_compaction")
val tablePath = s"$basePath/events_mor"

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))

def events(fromId: Long, n: Long, batch: Int): DataFrame =
  spark.range(fromId, fromId + n).select(
    concat(lit("e"), col("id")).as("id"),
    (col("id") % 1000).as("user_id"),
    expr("timestamp_millis(1767225600000 + id * 1000)").as("event_time"),
    when(col("id") % 2 === 0, "2026-01-01").otherwise("2026-01-02").as("datestr"),
    lit(batch.toLong).as("ts"),
    expr("repeat('x', 200)").as("payload"))

def completedActions(path: String): Seq[String] = {
  val mc = HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf()).setBasePath(path).build()
  mc.getActiveTimeline.filterCompletedInstants().getInstants.asScala.map(_.getAction).toSeq
}

val writeOpts = Map(
  "hoodie.table.name" -> "events_mor",
  "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.partitionpath.field" -> "datestr",
  "hoodie.table.ordering.fields" -> "ts",
  // Inline compaction + hybrid trigger
  "hoodie.compact.inline" -> "true",
  "hoodie.compact.inline.trigger.strategy" -> "NUM_OR_TIME",
  "hoodie.compact.inline.max.delta.commits" -> "10",
  "hoodie.compact.inline.max.delta.seconds" -> "1800",
  // Optional: cap IO if you must stay under a latency budget
  "hoodie.compaction.target.io" -> "20480")                    // MB, so 20 GB

def write(df: DataFrame, operation: String): Unit =
  df.write.format("hudi").options(writeOpts + ("hoodie.datasource.write.operation" -> operation)).mode("append").save(tablePath)

write(events(0L, 20000L, 1), "insert")

for (batch <- 2 to 11) {
  write(events(0L, 2000L, batch), "upsert")
  val actions = completedActions(tablePath)
  println(f"after write $batch%2d: ${actions.count(_ == "deltacommit")}%2d delta commits, " +
    s"${actions.count(_ == "commit")} compaction(s)")
}

println("\nCompleted instants on the active timeline:")
completedActions(tablePath).foreach(a => println(s"  $a"))

// Read-optimized queries read only base files: they show the upserts merged by the compaction,
// but not the ones written to log files after it. Snapshot queries show all of them.
val ro = spark.read.format("hudi").option("hoodie.datasource.query.type", "read_optimized").load(tablePath)
val snapshot = spark.read.format("hudi").load(tablePath)
println(s"\nLatest batch visible: read-optimized ts = ${ro.agg(max("ts")).first.get(0)}, " +
  s"snapshot ts = ${snapshot.agg(max("ts")).first.get(0)}")

System.exit(0)
