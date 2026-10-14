// Chapter 9, "Registering tables through Apache Polaris" - a second attempt that references
// the table through the fully-qualified `polaris_catalog.mydb.<table>` path, forcing Spark to
// route the CREATE TABLE straight through the real org.apache.polaris.spark.SparkCatalog
// plugin instance instead of through HoodieCatalog's DelegatingCatalogExtension-based
// createTable (see spark/polaris_hudi_table.scala's comment on why the bare `mydb.hudi_table`
// path from the docs never reaches Polaris at all in this build).
//
// This does reach Polaris - and hits a real bug: PolarisSparkCatalog.createTable crashes with
// java.util.NoSuchElementException: None.get inside
// org.apache.polaris.spark.utils.PolarisCatalogUtils.loadSparkTable (PolarisCatalogUtils.java:73),
// on org.apache.polaris:polaris-spark-3.5_2.12:1.3.0-incubating, for a `USING hudi` table.
// Recorded here as-is; not worked around, since there is no known session/table option that
// avoids it (Polaris's Spark client's CREATE TABLE path appears to assume an
// Iceberg/Delta-shaped table property set that a plain Hudi CREATE TABLE does not provide).

try {
  spark.sql("""
    CREATE TABLE IF NOT EXISTS polaris_catalog.mydb.hudi_table_qualified (
      id   INT,
      name STRING,
      ts   BIGINT,
      dt   STRING
    ) USING hudi
    PARTITIONED BY (dt)
    LOCATION 'file:///tmp/hudiinaction/chapter09/polaris_warehouse/mydb/hudi_table_qualified'
    TBLPROPERTIES (primaryKey = 'id')
  """)
  println("POLARIS_QUALIFIED_RESULT=UNEXPECTED_SUCCESS")
  spark.sql("SELECT * FROM polaris_catalog.mydb.hudi_table_qualified").show(false)
} catch {
  case e: Throwable =>
    println("POLARIS_QUALIFIED_RESULT=FAILED")
    println(s"Exception class: ${e.getClass.getName}")
    println(s"Message: ${e.getMessage}")
    e.getStackTrace.take(6).foreach(f => println(s"  at $f"))
}

println("POLARIS_QUALIFIED_DONE")
System.exit(0)
