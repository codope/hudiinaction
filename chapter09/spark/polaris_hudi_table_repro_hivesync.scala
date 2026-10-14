try {
  spark.sql("""
    CREATE TABLE IF NOT EXISTS polaris_catalog.mydb.hudi_table_v140 (
      id   INT,
      name STRING,
      ts   BIGINT,
      dt   STRING
    ) USING hudi
    PARTITIONED BY (dt)
    LOCATION 'file:///tmp/hudiinaction/chapter09/polaris_warehouse/mydb/hudi_table_v140'
    TBLPROPERTIES (primaryKey = 'id')
  """)
  println("POLARIS_V140_RESULT=UNEXPECTED_SUCCESS_CREATE")
  spark.sql("DESCRIBE EXTENDED polaris_catalog.mydb.hudi_table_v140").show(100,false)
  spark.sql("INSERT INTO polaris_catalog.mydb.hudi_table_v140 SELECT 1,'John',1695159649087L,'2024-01-01'")
  spark.sql("SELECT * FROM polaris_catalog.mydb.hudi_table_v140").show(false)
  println("POLARIS_V140_RESULT=FULL_SUCCESS")
} catch {
  case e: Throwable =>
    println("POLARIS_V140_RESULT=FAILED")
    println(s"Exception class: ${e.getClass.getName}")
    println(s"Message: ${e.getMessage}")
    e.getStackTrace.take(8).foreach(f => println(s"  at $f"))
}
println("POLARIS_V140_DONE")
System.exit(0)
