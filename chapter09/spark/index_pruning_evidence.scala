// Follow-up: "49 rows in 271ms" does not prove pruning. This builds a table with enough base
// files (25 insert batches, small.file.limit=0, so every batch becomes its own file) and
// records actual scan-level evidence for data skipping on vs. off:
//   1. SQL metrics off df.queryExecution.executedPlan's leaf scan node (e.g. "number of
//      files read") after collect(), compared with hoodie.enable.data.skipping/
//      hoodie.metadata.enable true vs false.
//   2. org.apache.hudi.HoodieFileIndex's own INFO-level log lines, which state total vs
//      candidate file counts directly when data skipping prunes files.

import java.io.File
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/index_pruning_evidence")
val tablePath = s"$basePath/rides"
val tableName = "ch9_index_pruning"

def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))
spark.sql(s"DROP TABLE IF EXISTS $tableName")

Logger.getLogger("org.apache.hudi.HoodieFileIndex").setLevel(Level.INFO)
Logger.getLogger("org.apache.hudi").setLevel(Level.WARN) // keep the rest quiet, just the file index at INFO

spark.sql(s"""
  CREATE TABLE $tableName (
      ts       BIGINT,
      id       STRING,
      rider    STRING,
      driver   STRING,
      fare     DOUBLE,
      city     STRING
  ) USING hudi
  OPTIONS (
      primaryKey = 'id',
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

val cities = Seq("san_francisco") // single partition, so file count differences are all about file-level pruning, not partition pruning
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
  }.toDF("ts", "id", "rider", "driver", "fare", "city")
  rows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "insert")
    // Table-level OPTIONS set at CREATE TABLE time (small.file.limit=0 above) are NOT
    // inherited by later plain df.write.format("hudi").save(path) calls -- confirmed
    // separately in this same follow-up pass (HoodieSparkSqlWriter.write() takes raw
    // optParams with no persisted-table-config merge). Must be repeated on every write.
    .option("hoodie.parquet.small.file.limit", "0")
    .option("hoodie.copyonwrite.insert.split.size", perBatch.toString)
    .mode(SaveMode.Append)
    .save(tablePath)
}
spark.sql(s"REFRESH TABLE $tableName")
val totalFiles = spark.sql(s"SELECT count(distinct _hoodie_file_name) as f FROM $tableName").collect()(0).getAs[Long]("f")
println(s"Loaded ${spark.table(tableName).count()} rows into $tableName across $totalFiles base files (single partition)")

spark.sql("SET hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider")
println("\n=== CREATE INDEX: secondary index on `driver` ===")
spark.sql(s"CREATE INDEX idx_driver ON $tableName (driver)")
println("=== CREATE INDEX: expression index on from_unixtime(ts, 'yyyy-MM-dd') ===")
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

// Using the catalog table name via spark.sql(...) let Spark cache the resolved relation /
// HoodieFileIndex across calls in the same session (visible as repeated "WARN CacheManager:
// Asked to cache already cached data" lines), which silently made ON and OFF runs reuse the
// FIRST run's already-pruned file list regardless of the new SET values -- a real, worth
// -reporting trap. Fixed by reading fresh via spark.read.format("hudi").options(...).load(path)
// on every call (per-call DataFrameReader options, no catalog-name caching) and by explicitly
// clearing the Spark cache and Hudi's own file-index cache between runs.
def runWithSkipping(label: String, enabled: Boolean, filterExpr: String): Unit = {
  spark.catalog.clearCache()
  println(s"\n--- $label (hoodie.enable.data.skipping=$enabled, hoodie.metadata.enable=$enabled) ---")
  println(s"Filter: $filterExpr")
  val reader = spark.read.format("hudi")
    .option("hoodie.enable.data.skipping", enabled.toString)
    .option("hoodie.metadata.enable", enabled.toString)
  val df = reader.load(tablePath).select("id", "driver", "fare").where(filterExpr)
  val n = df.count()
  println(s"Rows returned: $n")
  spark.catalog.clearCache()
  val df2 = reader.load(tablePath).select("id", "driver", "fare").where(filterExpr)
  df2.collect()
  scanMetrics(df2)
}

val secondaryIndexFilter = s"driver = '${drivers(0)}'"
println("\n############################################################")
println("# (a) Secondary-index lookup: driver = a specific value")
println("############################################################")
runWithSkipping("Data skipping ON", enabled = true, secondaryIndexFilter)
runWithSkipping("Data skipping OFF", enabled = false, secondaryIndexFilter)

val exprIndexFilter = "from_unixtime(ts, 'yyyy-MM-dd') = from_unixtime(1700000000, 'yyyy-MM-dd')"
println("\n############################################################")
println("# (b) Expression-index predicate: from_unixtime(ts, 'yyyy-MM-dd') = a specific day")
println("############################################################")
runWithSkipping("Data skipping ON", enabled = true, exprIndexFilter)
runWithSkipping("Data skipping OFF", enabled = false, exprIndexFilter)

// Reset for cleanliness
spark.sql("SET hoodie.enable.data.skipping=true")
spark.sql("SET hoodie.metadata.enable=true")

System.exit(0)
