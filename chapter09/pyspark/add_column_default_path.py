# Chapter 9, "Evolving the schema on write" - the DEFAULT (schema-on-read OFF) ADD COLUMNS path
# on a table with VECTOR + BLOB columns. PySpark version of spark/add_column_default_path.scala.
#
# HoodieCatalog.alterTable handles AddColumn/UpdateColumnType/UpdateColumnComment without going
# through the schema-on-read guard, so ADD COLUMNS works here even though schema-on-read itself
# cannot be turned on for a VECTOR-bearing table. This script does NOT set
# hoodie.schema.on.read.enable anywhere (session or table property).

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

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/add_column_default_path")
table_path = f"{base_path}/support_tickets"
table_name = "support_tickets_default_addcol"

shutil.rmtree(base_path, ignore_errors=True)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

print(f"spark.sql.catalogImplementation = {spark.conf.get('spark.sql.catalogImplementation')}")

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
      type = 'cow'
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
print("=== Baseline schema (schema-on-read NOT enabled anywhere, table holds VECTOR + BLOB) ===")
spark.table(table_name).printSchema()

print("\n=== ALTER TABLE ... ADD COLUMNS (priority STRING), default path (no schema-on-read) ===")
try:
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (priority STRING)")
    print("ADD COLUMNS SUCCEEDED.")
    spark.table(table_name).printSchema()
    spark.sql(f"""
      INSERT INTO {table_name} VALUES
        ('T-2', 'Refund request', 2000, {float_array([0.0, 0.9, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])}, {inline_blob("11223344")}, 'high')
    """)
    print("Insert using the new column succeeded. Reading back:")
    spark.sql(f"SELECT ticket_id, subject, priority, length(read_blob(thumbnail)) AS thumb_bytes "
              f"FROM {table_name} ORDER BY ticket_id").show(truncate=False)
except Exception as e:
    print(f"ADD COLUMNS FAILED: {root_cause(e)}")

print("ADD_COLUMN_DEFAULT_PATH_DONE")
