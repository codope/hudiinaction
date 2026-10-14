// Chapter 9 encryption example, part 3: (d) the same recipe on a MOR table, then check
// whether the resulting log file (Avro-based, not Parquet) is actually encrypted, or
// whether the encryption only ever covered the Parquet base files.

import java.io.File
import org.apache.spark.sql.SaveMode
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/encryption")
val morPath = s"$basePath/mor_rides"

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(morPath))

// Parquet modular encryption is activated via spark.hadoop.parquet.* launch-time confs
// in scripts/spark_shell_with_hudi_encryption.sh -- see encryption_cow.scala for why
// runtime hadoopConfiguration.set() does not work here.
val hadoopConf = spark.sparkContext.hadoopConfiguration

// A distinctive, greppable sentinel value so we can check the raw log-file bytes for
// plaintext leakage without needing an Avro reader.
val sentinel = "SENTINEL-FARE-13377331"

println("=== Writing a MOR table (base file), fare encrypted ===")
val rows = Seq(("r1", "rider-A", "NYC", 12.5)).toDF("id", "rider", "city", "fare")
try {
  rows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "bulk_insert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", "mor_rides")
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
    .mode(SaveMode.Overwrite)
    .save(morPath)
  println("Base-file write SUCCEEDED.")
} catch {
  case e: Exception => println(s"Base-file write FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println(s"\n=== Upserting rider name to a sentinel value, to force a MOR log file (no inline compaction) ===")
val updated = Seq(("r1", sentinel, "NYC", 12.5)).toDF("id", "rider", "city", "fare")
try {
  updated.write.format("hudi")
    .option("hoodie.datasource.write.operation", "upsert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", "mor_rides")
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
    .option("hoodie.compact.inline", "false")
    .mode(SaveMode.Append)
    .save(morPath)
  println("Upsert (log file write) SUCCEEDED.")
} catch {
  case e: Exception => println(s"Upsert (log file write) FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println("\n=== Reading back through Hudi (same session, keys configured) ===")
try {
  spark.read.format("hudi").load(morPath).select("id", "rider", "city", "fare").orderBy("id").show(false)
} catch {
  case e: Exception => println(s"Read FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println("\n=== Checking the on-disk files for the sentinel string in plaintext ===")
// Excludes .hoodie/ (the metadata table has its own, unrelated log/base files under
// .hoodie/metadata) -- only the actual data-table files are relevant here.
def allFiles(dir: File): Seq[File] =
  if (dir.isDirectory) dir.listFiles().filterNot(_.getName == ".hoodie").flatMap(allFiles).toSeq
  else Seq(dir)
val logFiles = allFiles(new File(morPath)).filter(f => f.getName.contains(".log.") && !f.getName.endsWith(".crc"))
val baseFiles = allFiles(new File(morPath)).filter(_.getName.endsWith(".parquet"))
println(s"Log files found: ${logFiles.map(_.getName).mkString(", ")}")
println(s"Base files found: ${baseFiles.map(_.getName).mkString(", ")}")

def containsPlaintext(f: File, needle: String): Boolean = {
  val bytes = java.nio.file.Files.readAllBytes(f.toPath)
  val needleBytes = needle.getBytes("UTF-8")
  bytes.containsSlice(needleBytes)
}
logFiles.foreach { f =>
  println(s"${f.getName}: sentinel found in plaintext = ${containsPlaintext(f, sentinel)}")
}
baseFiles.foreach { f =>
  println(s"${f.getName}: sentinel found in plaintext = ${containsPlaintext(f, sentinel)}")
}

System.exit(0)
