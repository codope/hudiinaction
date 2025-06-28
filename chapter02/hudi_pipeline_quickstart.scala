import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.types.DoubleType
import spark.implicits._


// The dataset for this chapter is New York Taxi dataset sample of one million rows.
// Untar the `chapter02/trips_0.gz` file to a location.
// Please change to the path where the source data is saved
val inputPath = "/Users/username/path/to/trips_0"
// Please change to the path where the Hudi table will be created
val basePath  = "/tmp/trips_table"

// read raw CSV (tab-separated)
val df = spark.read.format("csv").
  option("header", "true").
  option("sep",    "\t").
  load(inputPath).
  toDF()

// First create a CoW table
df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field",     "trip_id")
  .option("hoodie.datasource.write.partitionpath.field", "vendor_id")
  .option("hoodie.datasource.write.precombine.field",    "pickup_datetime")
  .option("hoodie.datasource.write.hive_style_partitioning", "true")
  .option("hoodie.table.name",                     "nyc_taxi_trips")
  .mode("Overwrite")
  .save(basePath)

// Smoke-test a read
val snapshotDf = spark.read.format("hudi").load(basePath)
snapshotDf.count() // 1000660 rows
snapshotDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").filter("vendor_id = '1'").limit(2).show()
// +----------+---------+-------------------+-----------+
// |   trip_id|vendor_id|    pickup_datetime|fare_amount|
// +----------+---------+-------------------+-----------+
// |1207977523|        1|2015-08-10 10:36:34|         16|
// |1208792114|        1|2015-08-11 06:17:47|        9.5|
// +----------+---------+-------------------+-----------+

// Build an update dataframe
val toUpsert = df.filter($"trip_id" === "1207977523").
  withColumn("fare_amount", $"fare_amount".cast(DoubleType) * 1.2).
  withColumn("pickup_datetime",   from_unixtime(unix_timestamp($"pickup_datetime") + 3600))

// Update
toUpsert.write.format("hudi")
  .option("hoodie.datasource.write.operation",  "upsert")
  .mode("Append")
  .save(basePath)

// read the record
val snapshotDf = spark.read.format("hudi").load(basePath)
snapshotDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").filter($"trip_id" === "1207977523").show()
// +----------+---------+-------------------+-----------+
// |   trip_id|vendor_id|    pickup_datetime|fare_amount|
// +----------+---------+-------------------+-----------+
// |1207977523|        1|2015-08-10 11:36:34|       19.2|
// +----------+---------+-------------------+-----------+

// Issue a delete
val toDelete = df.filter($"trip_distance".cast("double") < 0.1)
toDelete.count() // 7900 rows
toDelete.select("trip_id", "vendor_id", "pickup_datetime", "trip_distance").filter("vendor_id = '1'").limit(1).show()
// +----------+---------+-------------------+-------------+
// |   trip_id|vendor_id|    pickup_datetime|trip_distance|
// +----------+---------+-------------------+-------------+
// |1200001601|        1|2015-07-16 23:38:37|            0|
// +----------+---------+-------------------+-------------+

toDelete.write.format("hudi")
  .option("hoodie.datasource.write.operation", "delete") // key line
  .mode("Append")
  .save(basePath)

val snapshotDf = spark.read.format("hudi").load(basePath)
snapshotDf.count() // 1000660 - 7900 = 992760 rows
snapshotDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").filter($"trip_id" === "1200001601").show()
// +-------+---------+---------------+-----------+
// |trip_id|vendor_id|pickup_datetime|fare_amount|
// +-------+---------+---------------+-----------+
// +-------+---------+---------------+-----------+

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import scala.collection.JavaConversions._

// Build meta client
val meta = HoodieTableMetaClient.builder().setBasePath(basePath).
  setConf(new HadoopStorageConfiguration(spark.sessionState.newHadoopConf())).
  build()

// Get the completed commits
val commits = meta.getCommitsTimeline.filterCompletedInstants.
  getInstants.toList.map(_.getCompletionTime()).sorted

println(commits.mkString("Commits -> ", ", ", ""))
// Commits -> 20250622072149247, 20250622074915924, 20250622082539914

val firstCommit = commits.get(0)
val upsertCommit = commits.get(1)
val deleteCommit = commits.get(2)

// Read-optimized query
val roDf = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "read_optimized").
  load(basePath)
roDf.count()  // 992760 rows same as snapshot on CoW but will be different for MoR

// Incremental query
val incrDf = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "incremental").
  option("hoodie.datasource.read.begin.instanttime", upsertCommit).
  option("hoodie.datasource.read.end.instanttime",   deleteCommit).
  load(basePath)

// Note that begin instant time is inclusive,
// while end instant time is exclusive.
incrDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").show(false)
// +----------+---------+-------------------+-----------+
// |trip_id   |vendor_id|pickup_datetime    |fare_amount|
// +----------+---------+-------------------+-----------+
// |1207977523|1        |2015-08-10 11:36:34|19.2       |
// +----------+---------+-------------------+-----------+

// Time travel query
val ttDf = spark.read.format("hudi").
  option("as.of.instant", upsertCommit).   // point-in-time
  load("/tmp/trips_table")
ttDf.count()                // back to original row count 1000660
ttDf.select("trip_id", "vendor_id", "pickup_datetime", "fare_amount").filter($"trip_id" === "1200001601").show()
// +----------+---------+-------------------+-----------+
// |   trip_id|vendor_id|    pickup_datetime|fare_amount|
// +----------+---------+-------------------+-----------+
// |1200001601|        1|2015-07-16 23:38:37|          3|
// +----------+---------+-------------------+-----------+

// Create Merge-on-Read Table
val morBasePath = "/tmp/trips_table_mor"
df.write.format("hudi").
  option("hoodie.datasource.write.recordkey.field",     "trip_id").
  option("hoodie.datasource.write.partitionpath.field", "vendor_id").
  option("hoodie.datasource.write.precombine.field",    "pickup_datetime").
  option("hoodie.datasource.write.hive_style_partitioning", "true").
  // all other options remain same as CoW, just set the table type
  option("hoodie.datasource.write.table.type",     "MERGE_ON_READ").
  option("hoodie.table.name",                 "nyc_taxi_trips_mor").
  mode("Overwrite").
  save(morBasePath)

// Update MoR table w/o compaction
toUpsert.write.format("hudi")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.operation", "upsert")
  .mode("Append")
  .save(morBasePath)

// Delete MoR table w/o compaction
toDelete.write.format("hudi")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.operation", "delete")
  .mode("Append")
  .save(morBasePath)

// Snapshot query on MoR
val snapshotDf = spark.read.format("hudi").load(morBasePath)
snapshotDf.count() // 992760 rows

// Read-optimized query
val roDf = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "read_optimized").
  load(morBasePath)
roDf.count() // 1000660 rows

// Update MoR table with compaction
toUpsert.write.format("hudi")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.operation", "upsert")
  // inline compaction ON
  .option("hoodie.compact.inline", "true")
  .option("hoodie.compact.inline.max.delta.commits", "1")
  .mode("Append")
  .save(morBasePath)

// Cluster 10MB small files to larger 40MB files
df.limit(0).write.format("hudi")
  .option("hoodie.datasource.write.operation",                 "upsert")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.clustering.inline",                          "true")
  .option("hoodie.clustering.inline.max.commits",              "1")
  // Cluster 10mb files to 40mb file
  .option("hoodie.clustering.plan.strategy.small.file.limit",  "10485760")
  .option("hoodie.clustering.plan.strategy.target.file.max.bytes","41943040")
  // sort by pickup_date while clustering
  .option("hoodie.clustering.plan.strategy.sort.columns",      "pickup_date")
  .mode("Append")
  .save(morBasePath)

// Clean older file slices
df.limit(0).write.format("hudi")
  .option("hoodie.datasource.write.operation",    "upsert")
  .option("hoodie.clean.commits.retained",        "1")
  .option("hoodie.clean.automatic",               "true")
  .mode("Append")
  .save(morBasePath)
