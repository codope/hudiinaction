# Chapter 9, "Choosing a base file format for AI workloads" - two checks behind the chapter's
# Lance paragraph. PySpark version of spark/lance_merger_int8.scala.
#   1. Does a Lance table need a record-merger property at all, and if so, which config key
#      works: the canonical 'hoodie.write.record.merge.custom.implementation.classes', its
#      deprecated alias 'hoodie.datasource.write.record.merger.impls', or the docs' earlier
#      spelling 'hoodie.record.merger.impls', which is no config key at all?
#   2. Does a VECTOR(8, INT8) column fail on a Lance-backed table at write time?
# See the Scala version's header for the source references.

import os
import shutil

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# The innermost JVM exception behind a PySpark error, as "class: message".
def root_cause(e):
    j = getattr(e, "java_exception", None) or getattr(e, "_origin", None)
    if j is None:
        return f"{type(e).__name__}: {e}"
    while j.getCause() is not None:
        j = j.getCause()
    return f"{j.getClass().getName()}: {j.getMessage()}"

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/lance_merger_int8")
shutil.rmtree(base_path, ignore_errors=True)


def drop_and_create(table_name, path, extra_props):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"""
      CREATE TABLE {table_name} (
        ticket_id STRING,
        subject   STRING,
        embedding VECTOR(8)
      ) USING hudi
      TBLPROPERTIES (
        primaryKey = 'ticket_id',
        type = 'cow',
        'hoodie.table.base.file.format' = 'LANCE'
        {extra_props}
      )
      LOCATION '{path}'
    """)


def try_insert(table_name):
    try:
        spark.sql(f"""
          INSERT INTO {table_name} VALUES
            ('T-1', 'test ticket', array(cast(0.1 as float), cast(0.2 as float), cast(0.0 as float),
                                          cast(0.0 as float), cast(0.0 as float), cast(0.0 as float),
                                          cast(0.0 as float), cast(0.0 as float)))
        """)
        return "SUCCEEDED"
    except Exception as e:
        return f"FAILED: {root_cause(e)}"


merger = "'org.apache.hudi.DefaultSparkRecordMerger'"

print("=== (1) No merger property at all ===")
drop_and_create("lance_no_merger", f"{base_path}/lance_no_merger", "")
print("Insert result: " + try_insert("lance_no_merger"))

print("\n=== (2) 'hoodie.record.merger.impls' (the docs' earlier spelling, not a config key) ===")
drop_and_create("lance_docs_key", f"{base_path}/lance_docs_key", f", 'hoodie.record.merger.impls' = {merger}")
print("Insert result: " + try_insert("lance_docs_key"))

print("\n=== (3) 'hoodie.datasource.write.record.merger.impls' (deprecated alias key) ===")
drop_and_create("lance_deprecated_key", f"{base_path}/lance_deprecated_key",
                f", 'hoodie.datasource.write.record.merger.impls' = {merger}")
print("Insert result: " + try_insert("lance_deprecated_key"))

print("\n=== (4) 'hoodie.write.record.merge.custom.implementation.classes' (canonical key, the chapter's) ===")
drop_and_create("lance_canonical_key", f"{base_path}/lance_canonical_key",
                f", 'hoodie.write.record.merge.custom.implementation.classes' = {merger}")
print("Insert result: " + try_insert("lance_canonical_key"))

print("\n=== (5) The chapter's support_tickets_lance DDL ===")
spark.sql("DROP TABLE IF EXISTS support_tickets_lance")
spark.sql(f"""
  CREATE TABLE support_tickets_lance (ticket_id STRING, subject STRING, embedding VECTOR(8))
  USING hudi
  TBLPROPERTIES (
     primaryKey = 'ticket_id', type = 'cow',
     'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger',
     'hoodie.table.base.file.format' = 'LANCE'
  )
  LOCATION '{base_path}/support_tickets_lance'
""")
print("Insert result against support_tickets_lance: " + try_insert("support_tickets_lance"))

print("\n=== (6) VECTOR(8, INT8) column on a Lance-backed table ===")
spark.sql("DROP TABLE IF EXISTS lance_int8")
try:
    spark.sql(f"""
      CREATE TABLE lance_int8 (
        ticket_id STRING,
        embedding VECTOR(8, INT8)
      ) USING hudi
      TBLPROPERTIES (
        primaryKey = 'ticket_id',
        type = 'cow',
        'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger',
        'hoodie.table.base.file.format' = 'LANCE'
      )
      LOCATION '{base_path}/lance_int8'
    """)
    print("CREATE TABLE lance_int8 SUCCEEDED.")
    try:
        spark.sql("""
          INSERT INTO lance_int8 VALUES
            ('T-1', array(cast(10 as tinyint), cast(20 as tinyint), cast(0 as tinyint), cast(0 as tinyint),
                           cast(0 as tinyint), cast(0 as tinyint), cast(0 as tinyint), cast(0 as tinyint)))
        """)
        print("INSERT into VECTOR(8, INT8) Lance table SUCCEEDED.")
    except Exception as e:
        print(f"INSERT into VECTOR(8, INT8) Lance table FAILED: {root_cause(e)}")
except Exception as e:
    print(f"CREATE TABLE lance_int8 FAILED: {root_cause(e)}")

print("LANCE_MERGER_INT8_DONE")
