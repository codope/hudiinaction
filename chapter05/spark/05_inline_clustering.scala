// Chapter 5, "Configuring Inline Clustering" - inline clustering every 4 commits, which bin-packs
// small files toward ~1 GiB and sorts the rewritten files by event_time and user_id.
//
// To get small files to cluster, this example turns off write-time small-file handling
// (hoodie.parquet.small.file.limit=0), so each insert adds a new file group per partition.
// The 4th commit then runs clustering, which replaces them with one sorted file group per
// partition and lands on the timeline as a replacecommit.

import java.io.File
import scala.collection.JavaConverters._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter05/inline_clustering")
val tablePath = s"$basePath/events_clustered"

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))

// Rows arrive in random order, so the sort done by clustering is visible.
def events(fromId: Long, n: Long, batch: Int): DataFrame =
  spark.range(fromId, fromId + n).select(
    concat(lit("e"), col("id")).as("id"),
    (rand(batch) * 1000).cast("long").as("user_id"),
    expr("timestamp_millis(1767225600000 + cast(rand() * 86400000 as long))").as("event_time"),
    when(col("id") % 2 === 0, "2026-01-01").otherwise("2026-01-02").as("datestr"),
    lit(batch.toLong).as("ts"),
    expr("repeat('x', 200)").as("payload"))

// File groups in the latest snapshot, per partition.
def latestFileGroups(path: String): Map[String, Int] = {
  spark.read.format("hudi").load(path)
    .groupBy("_hoodie_partition_path").agg(countDistinct("_hoodie_file_name").as("files"))
    .collect().map(r => r.getString(0) -> r.getLong(1).toInt).toMap
}

def completedActions(path: String): Seq[String] = {
  val mc = HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf()).setBasePath(path).build()
  mc.getActiveTimeline.filterCompletedInstants().getInstants.asScala.map(_.getAction).toSeq
}

val writeOpts = Map(
  "hoodie.table.name" -> "events_clustered",
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.partitionpath.field" -> "datestr",
  "hoodie.table.ordering.fields" -> "ts",
  "hoodie.datasource.write.operation" -> "insert",
  "hoodie.parquet.small.file.limit" -> "0",                   // demo only: produce small files
  // turn on inline (synchronous) clustering; alternatively use async in a companion job
  "hoodie.clustering.inline" -> "true",
  "hoodie.clustering.inline.max.commits" -> "4",
  // plan: pick small files and group them to hit the target size
  "hoodie.clustering.plan.strategy.small.file.limit" -> "629145600",        // ~600 MB
  "hoodie.clustering.plan.strategy.target.file.max.bytes" -> "1073741824",  // ~1 GiB
  // execution: sort-by hot columns while writing
  "hoodie.clustering.execution.strategy.class" ->
    "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
  "hoodie.clustering.plan.strategy.sort.columns" -> "event_time,user_id")

for (batch <- 1 to 4) {
  events((batch - 1) * 20000L, 20000L, batch).write.format("hudi").options(writeOpts).mode("append").save(tablePath)
  println(s"after commit $batch: file groups per partition ${latestFileGroups(tablePath)}, " +
    s"replacecommits ${completedActions(tablePath).count(_ == "replacecommit")}")
}

println(s"\nRows after clustering: ${spark.read.format("hudi").load(tablePath).count()} (4 x 20,000)")

System.exit(0)
