// Chapter 9, "Encrypting columns at rest" - the Parquet modular encryption recipe from
// encryption.md, on a COW table: write with keys configured, read back with keys (should
// work), then an upsert to see whether Hudi's merge-on-write path tolerates encrypted
// columns. A SEPARATE spark-shell session (encryption_read_no_keys.scala, run via
// scripts/run_encryption.sh) checks the "read without keys" and MOR cases.
//
// encryption.md dates from 2022 and only ever documented COW; this and the sibling scripts
// are what decide whether the book can still claim this recipe works at 1.2.0.

import java.io.File
import org.apache.spark.sql.SaveMode
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/encryption")
val cowPath = s"$basePath/cow_rides"

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))

// Parquet modular encryption is activated via spark.hadoop.parquet.* launch-time confs
// in scripts/spark_shell_with_hudi_encryption.sh, not mutated here at runtime: setting
// these directly on spark.sparkContext.hadoopConfiguration from inside a running
// spark-shell session was tried first and did NOT reach the Parquet writer (every write
// failed with "Could not instantiate KmsClient class: null", i.e. the config value never
// arrived). Launch-time spark.hadoop.* confs do reach it. Print them here to confirm.
val hadoopConf = spark.sparkContext.hadoopConfiguration
println("=== Effective Hadoop crypto config (from spark.hadoop.* launch confs) ===")
Seq("parquet.crypto.factory.class", "parquet.encryption.kms.client.class",
    "parquet.encryption.footer.key", "parquet.encryption.column.keys").foreach { k =>
  println(s"$k = ${hadoopConf.get(k)}")
}

println("=== Writing a COW table with fare encrypted (parquet.encryption.column.keys=k2:fare) ===")
val rows = Seq(("r1", "rider-A", "NYC", 12.5), ("r2", "rider-B", "SF", 22.75)).toDF("id", "rider", "city", "fare")
try {
  rows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "bulk_insert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", "cow_rides")
    .mode(SaveMode.Overwrite)
    .save(cowPath)
  println("Write SUCCEEDED.")
} catch {
  case e: Exception => println(s"Write FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

// ============================================================================
// (a) Read it back WITH the crypto settings still active -- should work.
// ============================================================================
println("\n=== (a) Reading back in the SAME session (keys still configured) ===")
try {
  spark.read.format("hudi").load(cowPath).select("id", "rider", "city", "fare").orderBy("id").show(false)
  println("Read WITH keys SUCCEEDED.")
} catch {
  case e: Exception => println(s"Read WITH keys FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

// ============================================================================
// (c) Try an upsert against the encrypted table (still with keys configured: an upsert
//     has to read the existing (encrypted) base file to merge, so this needs the keys
//     regardless of whether the plain read in (b) has them).
// ============================================================================
println("\n=== (c) Upsert against the encrypted table (keys configured) ===")
val updated = Seq(("r1", "rider-A", "NYC", 99.99)).toDF("id", "rider", "city", "fare")
try {
  updated.write.format("hudi")
    .option("hoodie.datasource.write.operation", "upsert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", "cow_rides")
    .mode(SaveMode.Append)
    .save(cowPath)
  println("Upsert SUCCEEDED.")
  spark.read.format("hudi").load(cowPath).select("id", "rider", "city", "fare").orderBy("id").show(false)
} catch {
  case e: Exception => println(s"Upsert FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

System.exit(0)
