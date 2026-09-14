// Chapter 9: checks what the bootstrap produced.
//
// Three things to look for:
//   1. Every source row is queryable through the Hudi table.
//   2. The metadata-only base files hold only the Hudi metadata columns, while the
//      full-record ones hold the data columns too.
//   3. hoodie.properties records the bootstrap base path, which is the dependency
//      that "Living with a bootstrapped table" warns about.

val tablePath = sys.env.getOrElse("TABLE_PATH", "/tmp/hudiinaction/chapter09/hudi/trips")

val df = spark.read.format("hudi").load(tablePath)

println("=== Row count by partition ===")
df.groupBy("trip_date").count().orderBy("trip_date").show(false)

println("=== Hudi metadata columns, one row per partition ===")
df.select("_hoodie_commit_time", "_hoodie_partition_path", "_hoodie_file_name", "trip_id", "fare")
  .orderBy("_hoodie_partition_path", "trip_id")
  .show(false)

println("=== What each mode actually wrote ===")
// Read the base files directly, bypassing Hudi, to see the columns each one holds.
val metadataOnlyPartition = s"$tablePath/trip_date=2023-07-04"
val fullRecordPartition   = s"$tablePath/trip_date=2025-02-14"
println("METADATA_ONLY base file: " + spark.read.parquet(metadataOnlyPartition).columns.mkString(", "))
println("FULL_RECORD base file:   " + spark.read.parquet(fullRecordPartition).columns.mkString(", "))
println("The skeleton carries only the Hudi metadata columns. Its data columns are still in the")
println("source file, and the query above stitched the two together.")

println("=== Table properties recording the bootstrap ===")
scala.io.Source.fromFile(s"$tablePath/.hoodie/hoodie.properties")
  .getLines()
  .filter(l => l.startsWith("hoodie.table.base.file.format") ||
               l.contains("bootstrap") ||
               l.startsWith("hoodie.table.version") ||
               l.startsWith("hoodie.table.name"))
  .foreach(println)

System.exit(0)
