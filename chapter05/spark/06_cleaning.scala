// Chapter 5, "Configuring Cleaner policies" - the three cleaner configurations from the chapter.
//
// 1. Inline cleaning, KEEP_LATEST_COMMITS with 10 commits retained. Fifteen upserts rewrite the
//    same file group; the number of retained file versions grows, then stops growing once the
//    cleaner starts removing versions older than the retention window.
// 2. Async cleaning: the same policy, with deletion done in the background.
// 3. Time-based retention: KEEP_LATEST_BY_HOURS with 24 hours.
//
// The chapter calls the async switch hoodie.clean.async and the cadence switch
// hoodie.clean.max.commits; Hudi 1.2.1 still accepts them as aliases of
// hoodie.clean.async.enabled and hoodie.clean.trigger.max.commits.

import java.io.File
import scala.collection.JavaConverters._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter05/cleaning")

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
    lit("2026-01-01").as("datestr"),
    lit(batch.toLong).as("ts"),
    expr("repeat('x', 200)").as("payload"))

def baseFileVersions(tablePath: String): Int =
  new File(s"$tablePath/2026-01-01").listFiles().count(_.getName.endsWith(".parquet"))

def completedCleans(path: String): Int = {
  val mc = HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf()).setBasePath(path).build()
  mc.getActiveTimeline.filterCompletedInstants().getInstants.asScala.count(_.getAction == "clean")
}

val commonOpts = Map(
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.partitionpath.field" -> "datestr",
  "hoodie.table.ordering.fields" -> "ts",
  "hoodie.datasource.write.operation" -> "upsert")

// 1. Inline cleaning (Spark)
val inlineClean = Map(
  "hoodie.clean.automatic" -> "true",
  "hoodie.clean.policy" -> "KEEP_LATEST_COMMITS",
  "hoodie.clean.commits.retained" -> "10")                     // keep last 10 commits

// 2. Async cleaning
val asyncClean = Map(
  "hoodie.clean.automatic" -> "true",
  "hoodie.clean.async" -> "true")

// 3. Time-based retention
val byHours = Map(
  "hoodie.clean.policy" -> "KEEP_LATEST_BY_HOURS",
  "hoodie.clean.hours.retained" -> "24")

val inlinePath = s"$basePath/events_clean_inline"
println("=== KEEP_LATEST_COMMITS, 10 commits retained ===")
for (batch <- 1 to 15) {
  events(0L, 5000L, batch).write.format("hudi").options(commonOpts ++ inlineClean)
    .option("hoodie.table.name", "events_clean_inline").mode("append").save(inlinePath)
  println(f"after commit $batch%2d: ${baseFileVersions(inlinePath)}%2d base file versions, " +
    s"${completedCleans(inlinePath)} clean(s)")
}

// The other two configurations, each on its own table. The async cleaner starts at the beginning
// of each write, so it runs one commit behind the inline cleaner. Time-based retention keeps every
// version written in the last 24 hours, so nothing is cleaned in this short run.
for ((name, opts) <- Seq("events_clean_async" -> (inlineClean ++ asyncClean), "events_clean_hours" -> byHours)) {
  val path = s"$basePath/$name"
  for (batch <- 1 to 15) {
    events(0L, 5000L, batch).write.format("hudi").options(commonOpts ++ opts)
      .option("hoodie.table.name", name).mode("append").save(path)
  }
  println(s"\n$name after 15 commits: ${baseFileVersions(path)} base file versions, ${completedCleans(path)} clean(s)")
}

System.exit(0)
