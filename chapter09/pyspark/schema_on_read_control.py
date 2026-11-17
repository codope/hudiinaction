# Chapter 9, "Enabling schema-on-read for everything else" - schema-on-read on a scalar-only
# table, with hoodie.schema.on.read.enable set in the session before the ALTER statements.
# PySpark version of spark/schema_on_read_control.scala. It is the control for
# schema_ai_types.py: if ADD COLUMNS, ALTER COLUMN TYPE and RENAME COLUMN all work here, a
# failure on the VECTOR + BLOB table is about those types.
#
# Before the rename it also sets hoodie.datasource.write.schema.allow.auto.evolution.column.drop,
# as the chapter's snippet does: without it, Hudi's guard against dropped columns sees the old
# name missing and the rename fails with MissingSchemaFieldException.

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

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/schema_on_read_control")
table_path = f"{base_path}/control_table"
table_name = "ch9_schema_control"

shutil.rmtree(base_path, ignore_errors=True)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

print("=== SET hoodie.schema.on.read.enable = true (session level, before any ALTER) ===")
spark.sql("SET hoodie.schema.on.read.enable=true")

spark.sql(f"""
  CREATE TABLE {table_name} (
      id         STRING,
      name       STRING,
      version_id INT
  ) USING hudi
  TBLPROPERTIES (
      primaryKey = 'id',
      orderingFields = 'id',
      type = 'cow'
  )
  LOCATION '{table_path}'
""")
spark.sql(f"INSERT INTO {table_name} VALUES ('r1', 'alice', 1)")
print("Baseline schema:")
spark.table(table_name).printSchema()

print("\n=== ALTER TABLE ... ADD COLUMNS (extra STRING) ===")
try:
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (extra STRING)")
    print("ADD COLUMNS SUCCEEDED.")
    spark.sql(f"INSERT INTO {table_name} VALUES ('r2', 'bob', 2, 'new-field')")
    spark.sql(f"SELECT * FROM {table_name} ORDER BY id").show(truncate=False)
except Exception as e:
    print(f"ADD COLUMNS FAILED: {root_cause(e)}")

print("\n=== ALTER TABLE ... ALTER COLUMN version_id TYPE BIGINT (int -> long) ===")
try:
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN version_id TYPE BIGINT")
    print("ALTER COLUMN TYPE SUCCEEDED.")
    spark.table(table_name).printSchema()
except Exception as e:
    print(f"ALTER COLUMN TYPE FAILED: {root_cause(e)}")

print("\n=== ALTER TABLE ... RENAME COLUMN name TO full_name ===")
spark.sql("SET hoodie.datasource.write.schema.allow.auto.evolution.column.drop=true")
try:
    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN name TO full_name")
    print("RENAME COLUMN SUCCEEDED.")
    spark.table(table_name).printSchema()
except Exception as e:
    print(f"RENAME COLUMN FAILED: {root_cause(e)}")
