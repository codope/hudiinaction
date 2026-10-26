// Chapter 9, "Looking up records by a non-key column" / "Indexing a derived value" - the
// chapter's exact queries, on a table named `rides` with a `ride_id` column (matching the
// chapter's prose exactly), reusing index_pruning_evidence.scala's method and its two fixes
// (small-file-limit not inherited by plain DataFrame writes; relation/FileIndex caching
// masking ON vs OFF differences when queried by catalog name in the same session).

import java.io.File
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/index_pruning_rides")
val tablePath = s"$basePath/rides"
val tableName = "rides"

def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))
spark.sql(s"DROP TABLE IF EXISTS $tableName")

Logger.getLogger("org.apache.hudi.HoodieFileIndex").setLevel(Level.INFO)
Logger.getLogger("org.apache.hudi").setLevel(Level.WARN)

spark.sql(s"""
  CREATE TABLE $tableName (
      ts       BIGINT,
      ride_id  STRING,
      rider    STRING,
      driver   STRING,
      fare     DOUBLE,
      city     STRING
  ) USING hudi
  OPTIONS (
      primaryKey = 'ride_id',
      hoodie.metadata.record.index.enable = 'true',
      hoodie.metadata.index.column.stats.enable = 'true',
      hoodie.metadata.index.column.stats.column.list = 'driver',
      hoodie.write.record.merge.mode = 'COMMIT_TIME_ORDERING',
      hoodie.parquet.small.file.limit = '0',
      hoodie.copyonwrite.record.size.estimate = '64',
      hoodie.parquet.max.file.size = '10240'
  )
  PARTITIONED BY (city)
  LOCATION '$tablePath'
""")

val cities = Seq("san_francisco")
val riders = (1 to 50).map(i => f"rider-$i%03d")
val drivers = (1 to 50).map(i => f"driver-$i%03d")
val rnd = new scala.util.Random(7)

val batches = 25
val perBatch = 40
for (b <- 0 until batches) {
  val rows = (0 until perBatch).map { i =>
    val city = cities(0)
    val rider = riders(rnd.nextInt(riders.size))
    val driver = drivers(rnd.nextInt(drivers.size))
    val fare = 5.0 + rnd.nextDouble() * 95.0
    val ts = 1700000000L + b * 100000L + i
    (ts, s"trip-$b-$i", rider, driver, fare, city)
  }.toDF("ts", "ride_id", "rider", "driver", "fare", "city")
  rows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "insert")
    .option("hoodie.parquet.small.file.limit", "0")
    .option("hoodie.copyonwrite.insert.split.size", perBatch.toString)
    .mode(SaveMode.Append)
    .save(tablePath)
}
spark.sql(s"REFRESH TABLE $tableName")
val totalFiles = spark.sql(s"SELECT count(distinct _hoodie_file_name) as f FROM $tableName").collect()(0).getAs[Long]("f")
println(s"Loaded ${spark.table(tableName).count()} rows into $tableName across $totalFiles base files (single partition)")

// The chapter queries a fixed date. Batch 10 starts at 2023-11-26 12:00 UTC, so its 40 rows fall on
// that calendar day in every session time zone between UTC-11 and UTC+11.
val aDatePresent = "2023-11-26"
println(s"Date used in the chapter's exact query below: $aDatePresent")

spark.sql("SET hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider")
spark.sql(s"CREATE INDEX record_index ON $tableName (ride_id)")
spark.sql(s"CREATE INDEX idx_driver ON $tableName (driver)")
spark.sql(s"""
  CREATE INDEX idx_ts_date ON $tableName USING column_stats(ts)
  OPTIONS(expr='from_unixtime', format='yyyy-MM-dd')
""")

def scanMetrics(df: org.apache.spark.sql.DataFrame): Unit = {
  val plan = df.queryExecution.executedPlan
  plan.collectLeaves().foreach { leaf =>
    println(s"  leaf node: ${leaf.nodeName}")
    leaf.metrics.toSeq.sortBy(_._1).foreach { case (k, v) => println(s"    $k = ${v.value}") }
  }
}

def runWithSkipping(label: String, enabled: Boolean, selectCols: Seq[String], filterExpr: String, countOnly: Boolean): Unit = {
  spark.catalog.clearCache()
  println(s"\n--- $label (hoodie.enable.data.skipping=$enabled, hoodie.metadata.enable=$enabled) ---")
  val reader = spark.read.format("hudi")
    .option("hoodie.enable.data.skipping", enabled.toString)
    .option("hoodie.metadata.enable", enabled.toString)
  val filtered = reader.load(tablePath).select(selectCols.head, selectCols.tail: _*).where(filterExpr)
  if (countOnly) {
    val n = filtered.count()
    println(s"count(*) = $n")
  } else {
    filtered.show(false)
  }
  spark.catalog.clearCache()
  val filtered2 = reader.load(tablePath).select(selectCols.head, selectCols.tail: _*).where(filterExpr)
  filtered2.collect()
  scanMetrics(filtered2)
}

println("\n############################################################")
println("# CHAPTER'S EXACT QUERY 1: SELECT ride_id, fare FROM rides WHERE driver = 'driver-001'")
println("############################################################")
runWithSkipping("Data skipping ON", enabled = true, Seq("ride_id", "fare"), "driver = 'driver-001'", countOnly = false)
runWithSkipping("Data skipping OFF", enabled = false, Seq("ride_id", "fare"), "driver = 'driver-001'", countOnly = false)

println("\n############################################################")
println(s"# CHAPTER'S EXACT QUERY 2: SELECT count(*) FROM rides WHERE from_unixtime(ts, 'yyyy-MM-dd') = '$aDatePresent'")
println("############################################################")
runWithSkipping("Data skipping ON", enabled = true, Seq("ride_id", "ts"), s"from_unixtime(ts, 'yyyy-MM-dd') = '$aDatePresent'", countOnly = true)
runWithSkipping("Data skipping OFF", enabled = false, Seq("ride_id", "ts"), s"from_unixtime(ts, 'yyyy-MM-dd') = '$aDatePresent'", countOnly = true)

spark.sql("SET hoodie.enable.data.skipping=true")
spark.sql("SET hoodie.metadata.enable=true")

println("INDEX_PRUNING_RIDES_DONE")
System.exit(0)
