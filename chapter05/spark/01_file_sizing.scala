// Chapter 5, "Managing file sizing and small files" - write-time file sizing on a
// Copy-on-Write table, compared with bulk_insert, which skips it.
//
// Five small insert commits go into each table. With the book's sizing options, insert packs
// new rows into the existing small file of each partition, so the number of file groups stays
// at one per partition. bulk_insert creates new file groups on every commit, which is why the
// chapter recommends clustering after a bulk load (see 05_inline_clustering.scala).

import java.io.File
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter05/file_sizing")

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))

// Synthetic events: two daily partitions, a ~200-byte payload per row.
def events(fromId: Long, n: Long, batch: Int): DataFrame =
  spark.range(fromId, fromId + n).select(
    concat(lit("e"), col("id")).as("id"),
    (col("id") % 1000).as("user_id"),
    expr("timestamp_millis(1767225600000 + id * 1000)").as("event_time"),
    when(col("id") % 2 === 0, "2026-01-01").otherwise("2026-01-02").as("datestr"),
    lit(batch.toLong).as("ts"),
    expr("repeat('x', 200)").as("payload"))

// File groups per partition: a base file is <fileId>_<writeToken>_<instant>.parquet, and every
// version of a file group shares the fileId.
def fileGroups(tablePath: String): Map[String, Int] =
  new File(tablePath).listFiles().filter(d => d.isDirectory && !d.getName.startsWith(".")).map { p =>
    p.getName -> p.listFiles().map(_.getName).filter(_.endsWith(".parquet")).map(_.split("_")(0)).distinct.length
  }.toMap

val commonOpts = Map(
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.partitionpath.field" -> "datestr",
  "hoodie.table.ordering.fields" -> "ts")

// The book's sizing options. Values are numbers of bytes.
val sizingOpts = commonOpts ++ Map(
  "hoodie.table.name" -> "events",
  "hoodie.parquet.max.file.size" -> "125829120",              // ~120 MB
  "hoodie.parquet.small.file.limit" -> "104857600",           // ~100 MB
  "hoodie.copyonwrite.record.size.estimate" -> "1024",        // better first commit
  "hoodie.datasource.write.operation" -> "insert")

val sizedPath = s"$basePath/events"
println("=== insert with write-time file sizing ===")
for (batch <- 1 to 5) {
  events((batch - 1) * 20000L, 20000L, batch).write.format("hudi").options(sizingOpts).mode("append").save(sizedPath)
  println(s"after commit $batch, file groups per partition: ${fileGroups(sizedPath)}")
}

val bulkPath = s"$basePath/events_bulk"
val bulkOpts = commonOpts ++ Map(
  "hoodie.table.name" -> "events_bulk",
  "hoodie.datasource.write.operation" -> "bulk_insert")
println("\n=== bulk_insert, which skips write-time file sizing ===")
for (batch <- 1 to 5) {
  events((batch - 1) * 20000L, 20000L, batch).write.format("hudi").options(bulkOpts).mode("append").save(bulkPath)
  println(s"after commit $batch, file groups per partition: ${fileGroups(bulkPath)}")
}

println(s"\nRows: insert table ${spark.read.format("hudi").load(sizedPath).count()}, " +
  s"bulk_insert table ${spark.read.format("hudi").load(bulkPath).count()}")

System.exit(0)
