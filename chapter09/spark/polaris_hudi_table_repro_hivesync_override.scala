try {
  spark.sql("""
    CREATE TABLE IF NOT EXISTS polaris_catalog.mydb.hudi_table_v140b (
      id   INT,
      name STRING,
      ts   BIGINT,
      dt   STRING
    ) USING hudi
    PARTITIONED BY (dt)
    LOCATION 'file:///tmp/hudiinaction/chapter09/polaris_warehouse/mydb/hudi_table_v140b'
    TBLPROPERTIES (primaryKey = 'id', 'hoodie.datasource.hive_sync.enable' = 'false')
  """)
  println("POLARIS_V140B_RESULT=CREATE_OK")
  spark.sql("INSERT INTO polaris_catalog.mydb.hudi_table_v140b SELECT 1,'John',1695159649087L,'2024-01-01'")
  spark.sql("INSERT INTO polaris_catalog.mydb.hudi_table_v140b SELECT 2,'Jane',1695159649088L,'2024-01-02'")
  println("=== SELECT ===")
  spark.sql("SELECT id, name, ts, dt FROM polaris_catalog.mydb.hudi_table_v140b ORDER BY id").show(false)
  println("POLARIS_V140B_RESULT=FULL_SUCCESS")
} catch {
  case e: Throwable =>
    println("POLARIS_V140B_RESULT=FAILED")
    println(s"Exception class: ${e.getClass.getName}")
    println(s"Message: ${e.getMessage}")
    e.getStackTrace.take(8).foreach(f => println(s"  at $f"))
}
println("POLARIS_V140B_DONE")
System.exit(0)
