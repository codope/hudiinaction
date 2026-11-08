// Chapter 5, "Scheduling compaction inline and executing asynchronously" - the writer only
// plans compaction (hoodie.compact.schedule.inline=true); a separate step executes the plan.
//
// After the default trigger of 5 delta commits, the writer leaves a compaction plan in the
// REQUESTED state. Here CALL run_compaction executes it; in production this is a separate job,
// such as HoodieCompactor, which may need a lock provider.
//
// The Spark datasource turns inline compaction on by default for Merge-on-Read batch writes, and
// Hudi rejects inline and schedule-only compaction together, so the writer sets
// hoodie.compact.inline=false explicitly.

import java.io.File
import scala.collection.JavaConverters._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter05/schedule_compaction")
val tablePath = s"$basePath/events_mor_sched"

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

def metaClient(path: String) =
  HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf()).setBasePath(path).build()

def pendingCompactions(path: String): Seq[String] =
  metaClient(path).getActiveTimeline.filterPendingCompactionTimeline().getInstants.asScala.map(_.toString).toSeq

def completedCompactions(path: String): Int =
  metaClient(path).getActiveTimeline.filterCompletedInstants().getInstants.asScala.count(_.getAction == "commit")

val writeOpts = Map(
  "hoodie.table.name" -> "events_mor_sched",
  "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.partitionpath.field" -> "datestr",
  "hoodie.table.ordering.fields" -> "ts",
  "hoodie.compact.inline" -> "false",                           // overrides the MoR default
  "hoodie.compact.schedule.inline" -> "true")                   // plans only

def write(df: DataFrame, operation: String): Unit =
  df.write.format("hudi").options(writeOpts + ("hoodie.datasource.write.operation" -> operation)).mode("append").save(tablePath)

write(events(0L, 20000L, 1), "insert")
for (batch <- 2 to 6) {
  write(events(0L, 2000L, batch), "upsert")
}

println(s"Pending compaction plans after 6 writes: ${pendingCompactions(tablePath).mkString(", ")}")
println(s"Completed compactions: ${completedCompactions(tablePath)}")

println("\n=== Executing the scheduled plan ===")
spark.sql(s"CALL run_compaction(op => 'run', path => '$tablePath')").show(false)

println(s"Pending compaction plans: ${pendingCompactions(tablePath).mkString(", ")}")
println(s"Completed compactions: ${completedCompactions(tablePath)}")

System.exit(0)
