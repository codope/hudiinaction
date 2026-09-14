// Chapter 9, "Running a bootstrap" - adopts the existing Parquet as a Hudi table.
//
// This is the chapter's snippet with two changes: local paths instead of s3://, and
// spark.emptyDataFrame as the write source. A bootstrap reads its data from
// hoodie.bootstrap.base.path, not from the DataFrame, so the DataFrame carries no rows.

import org.apache.spark.sql.SaveMode

val sourcePath = sys.env.getOrElse("SOURCE_PATH", "/tmp/hudiinaction/chapter09/legacy/trips")
val tablePath  = sys.env.getOrElse("TABLE_PATH",  "/tmp/hudiinaction/chapter09/hudi/trips")

spark.emptyDataFrame.write.format("hudi").
  option("hoodie.datasource.write.operation", "bootstrap").
  option("hoodie.bootstrap.base.path", sourcePath).
  option("hoodie.bootstrap.mode.selector",
    "org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector").
  option("hoodie.bootstrap.mode.selector.regex", "trip_date=202[0-4]-.*").
  option("hoodie.bootstrap.mode.selector.regex.mode", "METADATA_ONLY").
  option("hoodie.datasource.write.recordkey.field", "trip_id").
  option("hoodie.datasource.write.partitionpath.field", "trip_date").
  // A full-record bootstrap reads the ordering field for every source row and does not
  // guard against it being unset: SparkFullBootstrapDataProviderBase passes it straight to
  // HoodieAvroUtils.getNestedFieldValAsString, which throws a NullPointerException. Any
  // bootstrap that writes full records therefore needs an ordering field.
  option("hoodie.table.ordering.fields", "updated_at").
  // The default of 1500 is sized for a cluster; this dataset is eight rows.
  option("hoodie.bootstrap.parallelism", "4").
  option("hoodie.datasource.write.hive_style_partitioning", "true").
  // A bootstrap cannot build the column-stats index: SparkBootstrapCommitActionExecutor
  // throws "col stats is not supported with bootstrap operation". The key defaults to
  // true on Spark, so it has to be turned off here.
  option("hoodie.metadata.index.column.stats.enable", "false").
  option("hoodie.table.name", "trips").
  mode(SaveMode.Overwrite).
  save(tablePath)

println(s"Bootstrapped $sourcePath into $tablePath")
println("Partitions matching trip_date=202[0-4]-.* are METADATA_ONLY; the rest are FULL_RECORD.")

System.exit(0)
