# Chapter 9, "Registering tables through Apache Polaris" - the working recipe: Polaris Spark
# client 1.4.0 + spark.sql.catalogImplementation=in-memory, against a fully-qualified
# polaris_catalog.mydb.* table name with an explicit LOCATION. PySpark version of
# spark/polaris_hudi_table_working.scala. `scripts/run_pyspark.sh polaris_hudi_table_working`
# sets the catalog options; start and bootstrap Polaris first (see the README).

import traceback

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

try:
    spark.sql("""
      CREATE TABLE IF NOT EXISTS polaris_catalog.mydb.hudi_rides (
        id   INT,
        name STRING,
        ts   BIGINT,
        dt   STRING
      ) USING hudi
      PARTITIONED BY (dt)
      LOCATION 'file:///tmp/hudiinaction/chapter09/polaris_warehouse/mydb/hudi_rides'
      TBLPROPERTIES (primaryKey = 'id')
    """)
    print("POLARIS_RESULT=CREATE_OK")
    spark.sql("INSERT INTO polaris_catalog.mydb.hudi_rides SELECT 1, 'John', 1695159649087L, '2024-01-01'")
    spark.sql("INSERT INTO polaris_catalog.mydb.hudi_rides SELECT 2, 'Jane', 1695159649088L, '2024-01-02'")
    print("=== SELECT ===")
    spark.sql("SELECT id, name, ts, dt FROM polaris_catalog.mydb.hudi_rides ORDER BY id").show(truncate=False)
    print("POLARIS_RESULT=FULL_SUCCESS")
except Exception:
    print("POLARIS_RESULT=FAILED")
    traceback.print_exc()
print("POLARIS_DONE")
