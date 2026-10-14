// Chapter 9, "Storing semi-structured and unstructured data" - the docs state VARIANT columns
// are not supported on Lance-backed tables (sql_ddl.md #variant: "VARIANT columns are not
// supported on Lance-backed tables", and the 1.2 release notes: "VARIANT is not supported on
// Lance-backed tables. Use Parquet for tables that contain VARIANT columns."). This script
// records what actually happens when you try anyway, rather than taking the docs' word for it.
// Requires --packages org.lance:lance-spark-bundle-4.1_2.13:0.4.0 (added by
// scripts/run_variant_lance.sh), matching the coordinate the task specified and confirmed to
// exist on Maven Central under org/lance/lance-spark-bundle-4.1_2.13/0.4.0/.

val basePath  = sys.env.getOrElse("VARIANT_BASE", "/tmp/hudiinaction/chapter09")
val lancePath = s"$basePath/variant_lance/events"

spark.sql("DROP TABLE IF EXISTS events_lance")

println("=== Attempting CREATE TABLE ... VARIANT ... hoodie.table.base.file.format = 'lance' ===")
try {
  spark.sql(s"""
    CREATE TABLE events_lance (
        event_id  STRING,
        payload   VARIANT,
        ts        BIGINT
    ) USING hudi
    TBLPROPERTIES (
        primaryKey = 'event_id',
        orderingFields = 'ts',
        type = 'cow',
        'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger',
        hoodie.table.base.file.format = 'lance'
    )
    LOCATION '$lancePath'
  """)
  println("CREATE TABLE succeeded (unexpected per docs). Attempting insert...")
  spark.sql("""
    INSERT INTO events_lance VALUES
      ('e1', parse_json('{"latency_ms":42}'), 1000)
  """)
  println("INSERT succeeded too. Reading back:")
  spark.sql("SELECT * FROM events_lance").show(false)
  println("VARIANT_ON_LANCE_RESULT=UNEXPECTED_SUCCESS")
} catch {
  case e: Throwable =>
    println(s"VARIANT_ON_LANCE_RESULT=FAILED")
    println(s"Exception class: ${e.getClass.getName}")
    println(s"Message: ${e.getMessage}")
    var cause = e.getCause
    var depth = 0
    while (cause != null && depth < 5) {
      println(s"  Caused by [$depth]: ${cause.getClass.getName}: ${cause.getMessage}")
      cause = cause.getCause
      depth += 1
    }
}

println("VARIANT_LANCE_DONE")
System.exit(0)
