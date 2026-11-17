# Chapter 9, "Storing semi-structured and unstructured data" - the docs state VARIANT columns
# are not supported on Lance-backed tables (sql_ddl.md #variant: "VARIANT columns are not
# supported on Lance-backed tables", and the 1.2 release notes: "VARIANT is not supported on
# Lance-backed tables. Use Parquet for tables that contain VARIANT columns."). This script
# records what actually happens when you try anyway, rather than taking the docs' word for it.
# Requires org.lance:lance-spark-bundle-4.1_2.13:0.4.0, which `scripts/run_pyspark.sh
# variant_lance` adds. PySpark version of spark/variant_lance.scala.

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("VARIANT_BASE", "/tmp/hudiinaction/chapter09")
lance_path = f"{base_path}/variant_lance/events"

spark.sql("DROP TABLE IF EXISTS events_lance")


# Class and message of the JVM exception behind a PySpark error, then its causes.
def java_error_chain(e):
    j = getattr(e, "java_exception", None) or getattr(e, "_origin", None)
    if j is None:
        return [f"{type(e).__name__}: {e}"]
    chain = []
    while j is not None:
        chain.append(f"{j.getClass().getName()}: {j.getMessage()}")
        j = j.getCause()
    return chain


print("=== Attempting CREATE TABLE ... VARIANT ... hoodie.table.base.file.format = 'lance' ===")
try:
    spark.sql(f"""
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
      LOCATION '{lance_path}'
    """)
    print("CREATE TABLE succeeded (unexpected per docs). Attempting insert...")
    spark.sql("""
      INSERT INTO events_lance VALUES
        ('e1', parse_json('{"latency_ms":42}'), 1000)
    """)
    print("INSERT succeeded too. Reading back:")
    spark.sql("SELECT * FROM events_lance").show(truncate=False)
    print("VARIANT_ON_LANCE_RESULT=UNEXPECTED_SUCCESS")
except Exception as e:
    print("VARIANT_ON_LANCE_RESULT=FAILED")
    chain = java_error_chain(e)
    print(f"Exception: {chain[0]}")
    for depth, cause in enumerate(chain[1:6]):
        print(f"  Caused by [{depth}]: {cause}")

print("VARIANT_LANCE_DONE")
