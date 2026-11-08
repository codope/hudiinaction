// Chapter 5, "Recovering from failures and disasters" - rolling back a commit, creating a
// savepoint, and restoring the table to it.
//
// The chapter runs these steps in the Hudi CLI (the commands are in cli/hudi_cli_recovery.md).
// This script runs the same steps through Spark SQL procedures, which need no extra install:
//   commit rollback --commit <instant>      ->  CALL rollback_to_instant
//   savepoint create --commit <instant>     ->  CALL create_savepoint
//   savepoints show                         ->  CALL show_savepoints
//   savepoint rollback --savepoint <instant> ->  CALL rollback_to_savepoint
// As the chapter says, pause all writers before a restore.

import java.io.File
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter05/recovery")
val tablePath = s"$basePath/events_recovery"
val tableName = "events_recovery"

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))
spark.sql(s"DROP TABLE IF EXISTS $tableName")

def events(fromId: Long, n: Long, batch: Int, amount: Double): DataFrame =
  spark.range(fromId, fromId + n).select(
    concat(lit("e"), col("id")).as("id"),
    when(col("id") % 2 === 0, "2026-01-01").otherwise("2026-01-02").as("datestr"),
    lit(batch.toLong).as("ts"),
    lit(amount).as("amount"))

val writeOpts = Map(
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.partitionpath.field" -> "datestr",
  "hoodie.table.ordering.fields" -> "ts",
  "hoodie.datasource.write.operation" -> "upsert")

def write(df: DataFrame): String = {
  df.write.format("hudi").options(writeOpts).mode("append").save(tablePath)
  spark.sql(s"CALL show_commits(table => '$tableName', limit => 1)").first.getAs[String]("commit_time")
}

def state(label: String): Unit = {
  val df = spark.read.format("hudi").load(tablePath)
  val r = df.agg(count(lit(1)), sum("amount")).first
  println(f"$label%-40s rows = ${r.getLong(0)}%5d, sum(amount) = ${r.getDouble(1)}%.1f")
}

events(0L, 1000L, 1, 10.0).write.format("hudi").options(writeOpts).mode("append").save(tablePath)
// Procedures look the table up by name, so register it once it exists.
spark.sql(s"CREATE TABLE $tableName USING hudi LOCATION '$tablePath'")
state("commit 1: initial load")

val goodCommit = write(events(1000L, 1000L, 2, 10.0)); state("commit 2: second load")

println(s"\n=== Savepoint at commit 2 ($goodCommit) ===")
spark.sql(s"CALL create_savepoint(table => '$tableName', commit_time => '$goodCommit')").show(false)
spark.sql(s"CALL show_savepoints(table => '$tableName')").show(false)

val badCommit = write(events(0L, 2000L, 3, 999.0)); state("commit 3: bad update of every row")
println(s"\n=== Rolling back commit 3 ($badCommit) ===")
spark.sql(s"CALL rollback_to_instant(table => '$tableName', instant_time => '$badCommit')").show(false)
state("after rollback of commit 3")

write(events(2000L, 500L, 4, 10.0)); state("commit 4: new rows")
write(events(0L, 2000L, 5, -1.0)); state("commit 5: bad backfill")

println(s"\n=== Restoring to the savepoint at commit 2 ($goodCommit) ===")
spark.sql(s"CALL rollback_to_savepoint(table => '$tableName', instant_time => '$goodCommit')").show(false)
state("after restore to savepoint")

// Remove savepoints you no longer need, so cleaning and archival can proceed.
spark.sql(s"CALL delete_savepoint(table => '$tableName', instant_time => '$goodCommit')").show(false)

System.exit(0)
