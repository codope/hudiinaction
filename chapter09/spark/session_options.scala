// Chapter 9, "Setting Hudi options for a whole Spark session" - spark.hoodie.* (1.2.0),
// what it takes effect on, and its precedence against a per-write option.
//
// The observable config here is hive_style_partitioning: whether a partition directory
// is named "2024" (default) or "city=2024" (hive style) is visible directly in the
// on-disk layout, so it doesn't need extra instrumentation to prove the setting took
// effect.
//
// Three things this checks against the real 1.2.0 bundle, not just the docs:
//   1. SET spark.hoodie.<key> at session level takes effect on a SQL-driven write to a
//      catalog-managed Hudi table (INSERT INTO ...).
//   2. An explicit hoodie.<key> table property / per-write option overrides the
//      session-level spark.hoodie.<key> setting (per the precedence documented in
//      ProvidesHoodieConfig.combineOptions: explicit option > SET hoodie.* > SET
//      spark.hoodie.* > persisted table config > catalog properties > defaults).
//   3. Whether spark.hoodie.* also reaches a plain DataFrame-writer path
//      (df.write.format("hudi")...save(path)), which does not go through the Spark SQL
//      catalog/command layer that applies spark.hoodie.*.

import java.io.File
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/session_options")

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))
Seq("t1", "t2").foreach(t => spark.sql(s"DROP TABLE IF EXISTS ch9_session_$t"))
spark.sql("DROP TABLE IF EXISTS trips_by_city")

def partitionDirs(tablePath: String): Seq[String] =
  new File(tablePath).listFiles().filter(_.isDirectory).map(_.getName).filterNot(_.startsWith(".")).sorted.toSeq

// ============================================================================
// 0. CHAPTER'S EXACT SNIPPET, VERBATIM
// ============================================================================
println("=== CHAPTER'S EXACT SNIPPET, VERBATIM ===")
spark.sql("SET spark.hoodie.datasource.write.hive_style_partitioning=true")

val tripsByCityPath = s"$basePath/trips_by_city"
spark.sql(s"""
  CREATE TABLE trips_by_city (trip_id STRING, city STRING, fare DOUBLE)
  USING hudi PARTITIONED BY (city)
  TBLPROPERTIES (primaryKey = 'trip_id')
  LOCATION '$tripsByCityPath'
""")
println("Resolved schema after CREATE TABLE (note where `city`, the partition column, ends up):")
spark.table("trips_by_city").printSchema()

println("\n--- Attempt 1: positional VALUES exactly as printed in the chapter ---")
try {
  spark.sql("INSERT INTO trips_by_city VALUES ('t1', 'NYC', 12.5), ('t2', 'SF', 30.0)")
  println("Positional INSERT SUCCEEDED.")
} catch {
  case e: Exception => println(s"Positional INSERT FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println("\n--- Attempt 2: same values, with an explicit column list (trip_id, city, fare) ---")
spark.sql("INSERT INTO trips_by_city (trip_id, city, fare) VALUES ('t1', 'NYC', 12.5), ('t2', 'SF', 30.0)")
println("Explicit-column-list INSERT SUCCEEDED.")
println("Partition directories for trips_by_city: " + partitionDirs(tripsByCityPath).mkString(", "))

// ============================================================================
// 1. SET spark.hoodie.* at session level, observed on a SQL-driven catalog write
// ============================================================================
println("=== SET spark.hoodie.datasource.write.hive_style_partitioning = true ===")
spark.sql("SET spark.hoodie.datasource.write.hive_style_partitioning = true")

val t1Path = s"$basePath/t1"
spark.sql(s"""
  CREATE TABLE ch9_session_t1 (
      id   STRING,
      city STRING,
      fare DOUBLE
  ) USING hudi
  TBLPROPERTIES (primaryKey = 'id')
  PARTITIONED BY (city)
  LOCATION '$t1Path'
""")
spark.sql("INSERT INTO ch9_session_t1 (id, city, fare) VALUES ('r1', 'NYC', 12.0), ('r2', 'SF', 22.0)")
println(s"Partition directories for ch9_session_t1 (no explicit hive_style_partitioning set): " +
  partitionDirs(t1Path).mkString(", "))
println("(hive style would look like 'city=NYC'; non-hive style would look like 'NYC')")

// ============================================================================
// 2. An explicit per-table option overrides the session-level spark.hoodie.* setting
// ============================================================================
val t2Path = s"$basePath/t2"
spark.sql(s"""
  CREATE TABLE ch9_session_t2 (
      id   STRING,
      city STRING,
      fare DOUBLE
  ) USING hudi
  TBLPROPERTIES (
      primaryKey = 'id',
      'hoodie.datasource.write.hive_style_partitioning' = 'false'
  )
  PARTITIONED BY (city)
  LOCATION '$t2Path'
""")
spark.sql("INSERT INTO ch9_session_t2 (id, city, fare) VALUES ('r1', 'NYC', 12.0), ('r2', 'SF', 22.0)")
println(s"\nPartition directories for ch9_session_t2 (explicit hive_style_partitioning=false, " +
  s"session still has spark.hoodie...=true): " + partitionDirs(t2Path).mkString(", "))
println("(explicit table property should win: non-hive style, e.g. 'NYC')")

// ============================================================================
// 3. Does spark.hoodie.* reach a plain DataFrame-writer path?
// ============================================================================
val t3Path = s"$basePath/t3"
val df = Seq(("r1", "NYC", 12.0), ("r2", "SF", 22.0)).toDF("id", "city", "fare")
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.partitionpath.field", "city")
  .option("hoodie.table.name", "ch9_session_t3")
  .mode("overwrite")
  .save(t3Path)
val t3Msg = "\nPartition directories for a plain df.write.format('hudi') table t3 " +
  "(no explicit hive_style_partitioning option, session still has spark.hoodie...=true): " +
  partitionDirs(t3Path).mkString(", ")
println(t3Msg)
println("If spark.hoodie.* reached this path too, this would also be hive style ('city=NYC').")
println("If it stays non-hive style ('NYC'), spark.hoodie.* did not apply to the plain DataFrame writer.")

System.exit(0)
