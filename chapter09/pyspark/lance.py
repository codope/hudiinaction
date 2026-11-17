# Chapter 9, "Choosing a base file format for AI workloads" - the same shape of table as
# ai_types.py, but with hoodie.table.base.file.format=LANCE instead of the Parquet default.
# PySpark version of spark/lance.scala.
#
# Two things this script checks against the real bundle, not just the docs:
#   1. What happens to the column-stats and partition-stats metadata-table partitions when
#      the base file format is Lance (MetadataPartitionType gates both off for LANCE, per
#      hudi-common's MetadataPartitionType.java - this should be silent, not an error).
#   2. Whether hudi_vector_search() works against a Lance-backed table.
#
# Requires the external Lance Spark bundle in addition to the Hudi bundle, so run it through
# `scripts/run_pyspark.sh lance`, which adds org.lance:lance-spark-bundle-3.5_2.12:0.4.0.

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

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/lance")
table_path = f"{base_path}/support_tickets_lance"
table_name = "support_tickets_lance"

shutil.rmtree(base_path, ignore_errors=True)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# ============================================================================
# 1. CREATE TABLE with hoodie.table.base.file.format = 'LANCE'
# ============================================================================
# Lance base files require the Spark record type; DefaultSparkRecordMerger is the
# merger implementation that routes through it. Without it, Hudi's writer factory
# defaults to the AVRO record type, whose Lance writer is a stub.
spark.sql(f"""
  CREATE TABLE {table_name} (
      ticket_id  STRING,
      subject    STRING,
      category   STRING,
      created_at BIGINT,
      embedding  VECTOR(8) COMMENT 'subject embedding, default FLOAT element type',
      thumbnail  BLOB       COMMENT 'small screenshot thumbnail, stored INLINE'
  ) USING hudi
  TBLPROPERTIES (
      primaryKey = 'ticket_id',
      orderingFields = 'created_at',
      type = 'cow',
      'hoodie.table.base.file.format' = 'LANCE',
      'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger'
  )
  LOCATION '{table_path}'
""")
print(f"Created LANCE-backed table {table_name} at {table_path}")

reference_struct_type = "struct<external_path:string,offset:bigint,length:bigint,managed:boolean>"


def inline_blob(bytes_hex):
    return (f"named_struct('type','INLINE','data',cast(unhex('{bytes_hex}') as binary),"
            f"'reference',cast(null as {reference_struct_type}))")


# Java's String.hashCode, so the placeholder thumbnails match the Scala version byte for byte.
def java_string_hash(s):
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    return h - (1 << 32) if h >= (1 << 31) else h


tickets = [
    ("T-2001", "App crashes on login",         "bug",     2001, [0.9, 0.1, 0.0, 0.0, 0.2, 0.0, 0.1, 0.0]),
    ("T-2002", "Password reset email missing", "account", 2002, [0.1, 0.9, 0.1, 0.0, 0.0, 0.1, 0.0, 0.0]),
    ("T-2003", "Double-charged on invoice",    "billing", 2003, [0.0, 0.1, 0.9, 0.1, 0.0, 0.0, 0.0, 0.1]),
    ("T-2004", "App crashes on Android 14",    "bug",     2004, [0.85, 0.15, 0.0, 0.05, 0.15, 0.0, 0.1, 0.0]),
]
insert_rows = []
for ticket_id, subject, category, ts, emb in tickets:
    emb_lit = "array(" + ",".join(f"cast({v} as float)" for v in emb) + ")"
    thumb_hex = f"{abs(java_string_hash(ticket_id)) % 256:02x}" * 8
    insert_rows.append(f"('{ticket_id}','{subject}','{category}',{ts},{emb_lit},{inline_blob(thumb_hex)})")
spark.sql(f"INSERT INTO {table_name} VALUES " + ",\n".join(insert_rows))
print(f"Inserted {len(tickets)} rows into the LANCE table")

print("\n=== Query the Lance-backed table back through Hudi ===")
spark.sql(f"SELECT ticket_id, subject, category FROM {table_name} ORDER BY ticket_id").show(truncate=False)

# ============================================================================
# 2. What happened to column-stats / partition-stats in the metadata table?
# ============================================================================
print("\n=== hoodie.properties: base file format ===")
with open(f"{table_path}/.hoodie/hoodie.properties") as f:
    for line in f:
        if line.startswith("hoodie.table.base.file.format") or line.startswith("hoodie.table.metadata.partitions"):
            print(line.rstrip())

print("\n=== Metadata table partitions actually present on disk ===")
metadata_dir = f"{table_path}/.hoodie/metadata"
if os.path.exists(metadata_dir):
    for p in sorted(d for d in os.listdir(metadata_dir) if os.path.isdir(os.path.join(metadata_dir, d))):
        print(f"  {p}")
else:
    print("  (no .hoodie/metadata directory at all)")


# hoodie.table.metadata.partitions lists the metadata-table partitions that are built and
# available to readers; CALL show_table_properties reads it from hoodie.properties.
def table_property(key):
    rows = spark.sql(f"CALL show_table_properties(path => '{table_path}', limit => 1000)").collect()
    return next((r["value"] for r in rows if r["key"] == key), "")


def metadata_partition_available(partition):
    return partition in table_property("hoodie.table.metadata.partitions").split(",")


print("\n=== Explicitly asking for column-stats / partition-stats index availability ===")
print("Base file format: " + table_property("hoodie.table.base.file.format"))
print("COLUMN_STATS partition available: " + str(metadata_partition_available("column_stats")).lower())
print("PARTITION_STATS partition available: " + str(metadata_partition_available("partition_stats")).lower())

# Try to build the column-stats index anyway, to see whether Hudi errors, warns, or
# silently ignores the request when the base file format is Lance.
print("\n=== Attempting a write with hoodie.metadata.index.column.stats.enable=true on the Lance table ===")
try:
    spark.sql(f"""
      INSERT INTO {table_name} VALUES
        ('T-2005', 'Refund for duplicate charge', 'billing', 2005,
         array(cast(0.0 as float), cast(0.05 as float), cast(0.85 as float), cast(0.15 as float), cast(0.0 as float), cast(0.05 as float), cast(0.0 as float), cast(0.1 as float)),
         {inline_blob("ab" * 8)})
    """)
    print("Insert with default column-stats config succeeded (no exception).")
except Exception as e:
    print(f"Insert FAILED: {root_cause(e)}")
print("COLUMN_STATS partition available after that write: " + str(metadata_partition_available("column_stats")).lower())

# ============================================================================
# 3. Does hudi_vector_search() work on a Lance-backed table?
# ============================================================================
print("\n=== hudi_vector_search() over the LANCE table ===")
query_vec = "ARRAY(" + ",".join(str(v) for v in [0.88, 0.12, 0.0, 0.0, 0.18, 0.0, 0.1, 0.0]) + ")"
try:
    spark.sql(f"""
      SELECT ticket_id, subject, category, _hudi_distance
      FROM hudi_vector_search('{table_path}', 'embedding', {query_vec}, 3, 'cosine')
      ORDER BY _hudi_distance
    """).show(truncate=False)
    print("hudi_vector_search() SUCCEEDED against the Lance-backed table.")
except Exception as e:
    print(f"hudi_vector_search() FAILED: {root_cause(e)}")
