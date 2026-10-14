// Chapter 9, "Registering tables through Apache Polaris" - the CONFIRMED WORKING recipe
// (renamed from the investigation's "v140c" script): Polaris Spark client 1.4.0 +
// spark.sql.catalogImplementation=in-memory (set by scripts/run_polaris_hudi_table_working.sh),
// against a fully-qualified polaris_catalog.mydb.* table name with an explicit LOCATION. Both
// fixes are required together -- see the repro scripts for what fails without each one.
try {
  spark.sql("""
    CREATE TABLE IF NOT EXISTS polaris_catalog.mydb.hudi_table_v140c (
      id   INT,
      name STRING,
      ts   BIGINT,
      dt   STRING
    ) USING hudi
    PARTITIONED BY (dt)
    LOCATION 'file:///tmp/hudiinaction/chapter09/polaris_warehouse/mydb/hudi_table_v140c'
    TBLPROPERTIES (primaryKey = 'id')
  """)
  println("POLARIS_V140C_RESULT=CREATE_OK")
  spark.sql("INSERT INTO polaris_catalog.mydb.hudi_table_v140c SELECT 1,'John',1695159649087L,'2024-01-01'")
  spark.sql("INSERT INTO polaris_catalog.mydb.hudi_table_v140c SELECT 2,'Jane',1695159649088L,'2024-01-02'")
  println("=== SELECT ===")
  spark.sql("SELECT id, name, ts, dt FROM polaris_catalog.mydb.hudi_table_v140c ORDER BY id").show(false)
  println("POLARIS_V140C_RESULT=FULL_SUCCESS")
} catch {
  case e: Throwable =>
    println("POLARIS_V140C_RESULT=FAILED")
    println(s"Exception class: ${e.getClass.getName}")
    println(s"Message: ${e.getMessage}")
    e.getStackTrace.take(8).foreach(f => println(s"  at $f"))
}
println("POLARIS_V140C_DONE")
System.exit(0)
