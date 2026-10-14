// Chapter 9, "Registering tables through Apache Polaris" - runs catalog_polaris.md's own
// documented recipe verbatim: CREATE TABLE ... USING hudi against the bare, unqualified table
// name (mydb.hudi_table), relying on Hudi's CreateHoodieTableCommand to detect Polaris
// (HoodieSqlCommonUtils.isUsingPolarisCatalog) and delegate registration to
// org.apache.polaris.spark.SparkCatalog. Run with scripts/run_polaris_hudi_table.sh.
//
// RESULT OF ACTUALLY RUNNING THIS (both with and without pre-creating the "mydb" namespace in
// Polaris via scripts/run_polaris_bootstrap.sh - it made no difference): the CREATE TABLE
// statement returns without an exception and the Hudi table files ARE written to disk (under
// spark.sql.warehouse.dir), but the table is registered in *neither* the local Hive/session
// catalog *nor* in Polaris (confirmed empty via `GET /api/catalog/v1/hudi_catalog/namespaces/
// mydb/tables`). The DESCRIBE below then fails with TABLE_OR_VIEW_NOT_FOUND. HoodieCatalog
// extends Spark's own `DelegatingCatalogExtension`, whose `createTable` forwards to whatever
// Spark wired as spark_catalog's *built-in* delegate - which is always Spark's own default
// session catalog, not the separately-named "polaris_catalog" plugin - so on this bare,
// unqualified path the "delegate to Polaris" step documented in catalog_polaris.md never
// actually reaches Polaris. See polaris_hudi_table_qualified.scala for the path that does
// reach Polaris (by naming the catalog explicitly), and what happens there instead.
try {
  spark.sql("CREATE DATABASE IF NOT EXISTS mydb")

  spark.sql("""
    CREATE TABLE IF NOT EXISTS mydb.hudi_table (
      id   INT,
      name STRING,
      ts   BIGINT,
      dt   STRING
    ) USING hudi
    PARTITIONED BY (dt)
    TBLPROPERTIES (
      primaryKey = 'id'
    )
  """)

  println("=== Table created. Describe extended: ===")
  spark.sql("DESCRIBE EXTENDED mydb.hudi_table").show(100, false)

  spark.sql("""
    INSERT INTO mydb.hudi_table
    SELECT 1 AS id, 'John' AS name, 1695159649087L AS ts, '2024-01-01' AS dt
  """)
  spark.sql("""
    INSERT INTO mydb.hudi_table PARTITION(dt)
    SELECT 2 AS id, 'Jane' AS name, 1695159649088L AS ts, '2024-01-02' AS dt
  """)

  println("=== SELECT * FROM mydb.hudi_table ===")
  spark.sql("SELECT id, name, ts, dt FROM mydb.hudi_table ORDER BY id").show(false)
  println("POLARIS_HUDI_TABLE_RESULT=SUCCESS")
} catch {
  case e: Throwable =>
    println("POLARIS_HUDI_TABLE_RESULT=FAILED")
    println(s"Exception class: ${e.getClass.getName}")
    println(s"Message: ${e.getMessage}")
}

println("POLARIS_HUDI_TABLE_DONE")
System.exit(0)
