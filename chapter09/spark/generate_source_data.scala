// Chapter 9: creates the pre-existing Parquet dataset that the bootstrap adopts.
// Plain Spark Parquet, hive-style partitioned by trip_date. No Hudi involved here:
// this stands in for the years of Parquet a team already has on object storage.

import org.apache.spark.sql.SaveMode
import spark.implicits._

val sourcePath = sys.env.getOrElse("SOURCE_PATH", "/tmp/hudiinaction/chapter09/legacy/trips")

// Partitions spread either side of the regex boundary used by the bootstrap:
// trip_date=2020..2024 is cold history, 2025 onward is recent.
val trips = Seq(
  ("trip-0001", "rider-01", "NYC",     12.50, "2023-01-15"),
  ("trip-0002", "rider-02", "NYC",     31.00, "2023-01-15"),
  ("trip-0003", "rider-03", "Chicago", 18.75, "2023-07-04"),
  ("trip-0004", "rider-01", "Chicago", 22.10, "2024-03-09"),
  ("trip-0005", "rider-04", "Austin",   9.95, "2024-11-28"),
  ("trip-0006", "rider-05", "Austin",  41.20, "2025-02-14"),
  ("trip-0007", "rider-02", "NYC",     15.40, "2025-06-30"),
  ("trip-0008", "rider-06", "Denver",  27.65, "2025-06-30")
).toDF("trip_id", "rider_id", "city", "fare", "trip_date")

trips
  .write
  .mode(SaveMode.Overwrite)
  .partitionBy("trip_date")
  .parquet(sourcePath)

println(s"Wrote source Parquet to $sourcePath")
spark.read.parquet(sourcePath).groupBy("trip_date").count().orderBy("trip_date").show(false)

System.exit(0)
