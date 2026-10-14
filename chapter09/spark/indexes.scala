// Chapter 9, "Finding records by key, by value, and by similarity" - SQL index management
// on a table large enough to spread across several files: CREATE INDEX for a secondary
// index, an expression index, a column-stats index, and a bloom-filter index; SHOW INDEXES;
// REFRESH INDEX; DROP INDEX. Shows evidence the indexes are used (metadata table partitions,
// EXPLAIN, a secondary-index lookup).
//
// This is a NEW dataset from Ch2/Ch4's examples: a few thousand synthetic rides, generated
// here, spread over several partitions and small files so file-level pruning is visible.

import java.io.File
import org.apache.spark.sql.SaveMode
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/indexes")
val tablePath = s"$basePath/rides"
val tableName = "rides"

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))
spark.sql(s"DROP TABLE IF EXISTS $tableName")

// ============================================================================
// 1. A table with a few thousand rows across several partitions and small files
// ============================================================================
// column stats on `driver` + partition stats are enabled at create time. The record index
// flag below is not enough on its own: the inserts use df.write, which ignores the table's
// OPTIONS, so the record index is built explicitly with CREATE INDEX in section 2.
// secondary index requires COMMIT_TIME_ORDERING (see sql_ddl.md's "Create Secondary Index").
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
      hoodie.copyonwrite.record.size.estimate = '256'
  )
  PARTITIONED BY (city)
  LOCATION '$tablePath'
""")

val cities = Seq("san_francisco", "sunnyvale", "austin", "houston", "denver")
val riders = (1 to 50).map(i => f"rider-$i%03d")
val drivers = (1 to 50).map(i => f"driver-$i%03d")
val n = 3000
val rnd = new scala.util.Random(42)

// Several small insert batches per partition, so each partition ends up with more than
// one base file -- otherwise "files scanned" pruning is trivially 1 regardless of indexes.
val batches = 10
for (b <- 0 until batches) {
  val rows = (0 until n / batches).map { i =>
    val city = cities(rnd.nextInt(cities.size))
    val rider = riders(rnd.nextInt(riders.size))
    val driver = drivers(rnd.nextInt(drivers.size))
    val fare = 5.0 + rnd.nextDouble() * 95.0
    val ts = 1700000000L + b * 100000L + i
    (ts, s"trip-$b-$i", rider, driver, fare, city)
  }.toDF("ts", "id", "rider", "driver", "fare", "city")
  rows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "insert")
    .mode(SaveMode.Append)
    .save(tablePath)
}
spark.sql(s"REFRESH TABLE $tableName")
println(s"Loaded ${spark.table(tableName).count()} rows into $tableName across ${cities.size} partitions, $batches insert batches")

println("\n=== Files per partition before any index-driven pruning ===")
spark.sql(s"SELECT city, count(distinct _hoodie_file_name) as files, count(*) as rows FROM $tableName GROUP BY city ORDER BY city").show(false)

// ============================================================================
// 2. CREATE INDEX: secondary index, expression index, column-stats index, bloom-filter index
// ============================================================================
// CREATE INDEX builds asynchronously and needs a lock provider even for a single writer:
// HoodieSparkIndexClient.getWriteClient() refuses to proceed without one ("To create index
// asynchronously, multi-writer configurations need to be enabled..."). This is NOT a table
// config persisted from the CREATE TABLE OPTIONS above (lock provider is a writer-side
// setting, not part of table config), so it has to be set as a session-level SQL conf
// before running CREATE INDEX. InProcessLockProvider is enough since everything here runs
// in this one spark-shell JVM.
spark.sql("SET hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider")

// A secondary index resolves record keys through the record index, so build that first.
println("\n=== CREATE INDEX: record index on the record key `id` ===")
spark.sql(s"CREATE INDEX record_index ON $tableName (id)")

println("\n=== CREATE INDEX: secondary index on `driver` ===")
spark.sql(s"CREATE INDEX idx_driver ON $tableName (driver)")

println("\n=== CREATE INDEX: expression index on date_format(ts as timestamp, 'yyyy-MM-dd') ===")
// ts is a raw epoch-seconds BIGINT here, so drive the expression off from_unixtime(ts, fmt),
// matching the from_unixtime example in sql_ddl.md.
spark.sql(s"""
  CREATE INDEX idx_ts_date ON $tableName USING column_stats(ts)
  OPTIONS(expr='from_unixtime', format='yyyy-MM-dd')
""")

println("\n=== CHAPTER'S EXACT SHOW INDEXES, for exactly idx_driver + idx_ts_date + the config-enabled indexes (before any other CREATE INDEX) ===")
spark.sql(s"SHOW INDEXES FROM $tableName").show(false)

println("\n=== CREATE INDEX: column-stats index on `rider` (via SQL) ===")
// A bare, untransformed column_stats index cannot be created through CREATE INDEX: with
// no expr (or expr='identity'), CreateIndexCommand refuses with HoodieIndexException
// ("Column stats index without expression on any column can be created using datasource
// configs"). That path is what `hoodie.metadata.index.column.stats.column.list` in the
// CREATE TABLE OPTIONS above already used for `driver`. CREATE INDEX ... USING
// column_stats(...) always needs a real transform, which makes it an expression index in
// practice -- so this demonstrates a second expression index, on lower(rider).
spark.sql(s"CREATE INDEX idx_rider_lower ON $tableName USING column_stats(rider) OPTIONS(expr='lower')")

println("\n=== CREATE INDEX: bloom-filter index on lower(rider) ===")
spark.sql(s"CREATE INDEX idx_bloom_rider ON $tableName USING bloom_filters(rider) OPTIONS(expr='lower')")

// ============================================================================
// 3. SHOW INDEXES / REFRESH INDEX
// ============================================================================
println("\n=== SHOW INDEXES ===")
spark.sql(s"SHOW INDEXES FROM $tableName").show(false)

println("\n=== REFRESH INDEX idx_driver ===")
try {
  spark.sql(s"REFRESH INDEX idx_driver ON $tableName")
  println("REFRESH INDEX succeeded.")
} catch {
  case e: Exception => println(s"REFRESH INDEX FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

// ============================================================================
// 4. Evidence the indexes are used
// ============================================================================
println("\n=== Metadata table partitions on disk ===")
val metadataDir = new File(s"$tablePath/.hoodie/metadata")
if (metadataDir.exists()) metadataDir.listFiles().filter(_.isDirectory).map(_.getName).sorted.foreach(p => println(s"  $p"))

println("\n=== EXPLAIN for a query on the expression-indexed column ===")
spark.sql(s"""
  EXPLAIN SELECT id, driver, fare FROM $tableName WHERE from_unixtime(ts, 'yyyy-MM-dd') = '1970-01-20'
""").show(false)

println("\n=== EXPLAIN for a query on the secondary-indexed column ===")
spark.sql(s"EXPLAIN SELECT id, rider, fare FROM $tableName WHERE driver = '${drivers(0)}'").show(false)

println("\n=== Secondary-index lookup query (driver = a specific value) ===")
val before = System.nanoTime()
val hits = spark.sql(s"SELECT id, driver, city, fare FROM $tableName WHERE driver = '${drivers(0)}'")
val hitCount = hits.count()
val elapsedMs = (System.nanoTime() - before) / 1000000
println(s"driver = '${drivers(0)}' matched $hitCount rows in ${elapsedMs}ms")
hits.show(10, false)

println("\n=== Bloom-filter-indexed lookup (lower(rider) = a specific value) ===")
spark.sql(s"SELECT id, rider, fare FROM $tableName WHERE lower(rider) = '${riders(0)}'").show(5, false)

// ============================================================================
// 5. DROP INDEX
// ============================================================================
println("\n=== DROP INDEX idx_bloom_rider ===")
spark.sql(s"DROP INDEX idx_bloom_rider ON $tableName")
println("\n=== SHOW INDEXES after DROP ===")
spark.sql(s"SHOW INDEXES FROM $tableName").show(false)

System.exit(0)
