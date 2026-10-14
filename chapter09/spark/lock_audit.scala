// Chapter 9, "Guarding and announcing commits" / lock providers - v2 TODO: does
// StorageBasedLockProvider work on a local file:// table path at 1.2.0? Source
// (release-1.2.0, hudi-io/src/main/java/org/apache/hudi/storage/StorageSchemes.java:36):
//   FILE("file", false, true, null)   <- storageLockClass is null for the "file" scheme
// versus S3/GCS/WASB/ABFS, which all carry a real *StorageLockClient class. The lock class
// is resolved per-scheme in StorageBasedLockProvider.getLockServiceClassName (release-1.2.0,
// hudi-client/hudi-client-common/.../StorageBasedLockProvider.java:177-183), which throws
// HoodieNotSupportedException("No implementation of StorageLock supports this scheme: " + scheme)
// when StorageSchemes.getStorageLockImplementationIfExists(scheme) returns empty. This script
// confirms that by an actual run rather than source-reading alone.

import java.io.File
import org.apache.spark.sql.SaveMode
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/lock_audit")
val tablePath = s"$basePath/rides"

def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))

val rows = Seq(("r1", "NYC", 12.0)).toDF("id", "city", "fare")
rows.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.table.ordering.fields", "id")
  .option("hoodie.table.name", "rides")
  .mode(SaveMode.Overwrite)
  .save(tablePath)
println("Seed write succeeded.")

println("\n=== Attempting to write with hoodie.write.lock.provider=StorageBasedLockProvider on a local file:// path ===")
val moreRows = Seq(("r2", "SF", 20.0)).toDF("id", "city", "fare")
try {
  moreRows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "upsert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", "rides")
    .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.StorageBasedLockProvider")
    .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
    .mode(SaveMode.Append)
    .save(tablePath)
  println("Write with StorageBasedLockProvider on local file:// SUCCEEDED (unexpected per source reading).")
} catch {
  case e: Exception =>
    println(s"Write FAILED as expected.")
    println(s"Exception class:   ${e.getClass.getName}")
    println(s"Exception message: ${e.getMessage}")
    var cause = e.getCause
    while (cause != null) {
      println(s"  caused by: ${cause.getClass.getName}: ${cause.getMessage}")
      cause = cause.getCause
    }
}

println("LOCK_AUDIT_DONE")
System.exit(0)
