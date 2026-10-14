// Chapter 9, "Encrypting columns at rest" - v2 TODO: does the metadata table's column-stats
// partition leak plaintext min/max for an encrypted column? Uses the chapter's exact
// encrypted column, `rider` (not `fare`, which the original encryption_cow.scala example
// used), with column stats explicitly enabled for it. Run via
// scripts/run_encryption_column_stats.sh, which sets ENCRYPTION_COLUMN_KEYS=k2:rider.

import java.io.File
import org.apache.spark.sql.SaveMode
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/encryption_column_stats")
val tablePath = s"$basePath/cow_rides"

def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))

println("=== Writing a COW table with `rider` encrypted, column stats enabled for `rider` ===")
val rows = Seq(("r1", "rider-Alice", "NYC", 12.5), ("r2", "rider-Bob", "SF", 22.75), ("r3", "rider-Carol", "LA", 45.0)).toDF("id", "rider", "city", "fare")
try {
  rows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "bulk_insert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", "cow_rides_colstats")
    .option("hoodie.metadata.index.column.stats.enable", "true")
    .option("hoodie.metadata.index.column.stats.column.list", "rider")
    .mode(SaveMode.Overwrite)
    .save(tablePath)
  println("Write SUCCEEDED.")
} catch {
  case e: Exception => println(s"Write FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println("\n=== Confirm the base Parquet file is actually encrypted (footer read fails without keys is checked separately; here just list files) ===")
val dataDir = new File(tablePath)
def listParquet(dir: File): Unit = dir.listFiles().foreach { f =>
  if (f.isDirectory && !f.getName.startsWith(".")) listParquet(f)
  else if (f.getName.endsWith(".parquet")) println(s"  ${f.getPath}")
}
listParquet(dataDir)

println("\n=== Metadata table: does the column_stats partition exist? ===")
val metadataDir = new File(s"$tablePath/.hoodie/metadata")
if (metadataDir.exists()) metadataDir.listFiles().filter(_.isDirectory).map(_.getName).sorted.foreach(p => println(s"  $p"))

println("\n=== Reading the metadata table's raw HoodieMetadataRecord rows for columnName='rider' ===")
try {
  val metaDf = spark.read.format("hudi").load(s"$tablePath/.hoodie/metadata")
  metaDf.printSchema()
  println("\n--- rows where the column-stats payload names 'rider' ---")
  metaDf.createOrReplaceTempView("hoodie_metadata_raw")
  spark.sql("""
    SELECT columnStatsMetadata.fileName, columnStatsMetadata.columnName,
           columnStatsMetadata.minValue, columnStatsMetadata.maxValue,
           columnStatsMetadata.isDeleted
    FROM hoodie_metadata_raw
    WHERE columnStatsMetadata.columnName = 'rider'
  """).show(false)
} catch {
  case e: Exception => println(s"Metadata table read FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println("ENCRYPTION_COLUMN_STATS_DONE")
System.exit(0)
