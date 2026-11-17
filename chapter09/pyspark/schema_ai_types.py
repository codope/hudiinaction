# Chapter 9, "Enabling schema-on-read for everything else" - tries schema-on-read
# (hoodie.schema.on.read.enable=true) operations (add column, rename column) on a table
# that also holds VECTOR and BLOB columns, and records whether it works or the exact error.
# PySpark version of spark/schema_ai_types.scala.

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

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/schema_ai_types")
table_path = f"{base_path}/support_tickets"
table_name = "ch9_schema_ai_types"

shutil.rmtree(base_path, ignore_errors=True)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Set in the session, before the ALTER statements, as the chapter requires; the table
# property below alone does not route ALTER TABLE through schema-on-read.
print("=== SET hoodie.schema.on.read.enable = true (session level, before any ALTER) ===")
spark.sql("SET hoodie.schema.on.read.enable=true")

spark.sql(f"""
  CREATE TABLE {table_name} (
      ticket_id  STRING,
      subject    STRING,
      created_at BIGINT,
      embedding  VECTOR(8) COMMENT 'subject embedding',
      thumbnail  BLOB       COMMENT 'small screenshot thumbnail, stored INLINE'
  ) USING hudi
  TBLPROPERTIES (
      primaryKey = 'ticket_id',
      orderingFields = 'created_at',
      type = 'cow',
      'hoodie.schema.on.read.enable' = 'true'
  )
  LOCATION '{table_path}'
""")

reference_struct_type = "struct<external_path:string,offset:bigint,length:bigint,managed:boolean>"


def inline_blob(bytes_hex):
    return (f"named_struct('type','INLINE','data',cast(unhex('{bytes_hex}') as binary),"
            f"'reference',cast(null as {reference_struct_type}))")


def float_array(values):
    return "array(" + ", ".join(f"cast({v} as float)" for v in values) + ")"


spark.sql(f"""
  INSERT INTO {table_name} VALUES
    ('T-1', 'App crashes on login', 1000, {float_array([0.9, 0.1, 0.0, 0.0, 0.2, 0.0, 0.1, 0.0])}, {inline_blob("aabbccdd")})
""")
print("=== Baseline schema (schema-on-read enabled, table holds VECTOR + BLOB) ===")
spark.table(table_name).printSchema()

# ============================================================================
# 1. ADD COLUMN
# ============================================================================
print("\n=== ALTER TABLE ... ADD COLUMNS (priority STRING) ===")
try:
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (priority STRING)")
    print("ADD COLUMNS SUCCEEDED.")
    spark.table(table_name).printSchema()
    spark.sql(f"INSERT INTO {table_name} VALUES ('T-2', 'Refund request', 2000, "
              f"{float_array([0.0, 0.9, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])}, {inline_blob('11223344')}, 'high')")
    print("Insert after ADD COLUMNS succeeded. Rows:")
    spark.sql(f"SELECT ticket_id, subject, priority FROM {table_name} ORDER BY ticket_id").show(truncate=False)
except Exception as e:
    print(f"ADD COLUMNS FAILED: {root_cause(e)}")

# ============================================================================
# 2. RENAME COLUMN
# ============================================================================
print("\n=== ALTER TABLE ... RENAME COLUMN embedding TO subject_embedding ===")
try:
    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN embedding TO subject_embedding")
    print("RENAME COLUMN SUCCEEDED.")
    spark.table(table_name).printSchema()
    spark.sql(f"SELECT ticket_id, subject_embedding FROM {table_name} ORDER BY ticket_id").show(truncate=False)
except Exception as e:
    print(f"RENAME COLUMN FAILED: {root_cause(e)}")

# ============================================================================
# 3. RENAME COLUMN on the BLOB column, for completeness
# ============================================================================
print("\n=== ALTER TABLE ... RENAME COLUMN thumbnail TO thumbnail_bytes ===")
try:
    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN thumbnail TO thumbnail_bytes")
    print("RENAME COLUMN (BLOB) SUCCEEDED.")
    spark.table(table_name).printSchema()
except Exception as e:
    print(f"RENAME COLUMN (BLOB) FAILED: {root_cause(e)}")
