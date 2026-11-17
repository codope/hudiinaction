# Chapter 5, "Managing file sizing and small files" - write-time file sizing on a
# Copy-on-Write table, compared with bulk_insert, which skips it. PySpark version of
# spark/01_file_sizing.scala.
#
# Five small insert commits go into each table. With the book's sizing options, insert packs
# new rows into the existing small file of each partition, so the number of file groups stays
# at one per partition. bulk_insert creates new file groups on every commit, which is why the
# chapter recommends clustering after a bulk load (see 05_inline_clustering.py).

import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, expr, lit, when

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter05/file_sizing")
shutil.rmtree(base_path, ignore_errors=True)


# Synthetic events: two daily partitions, a ~200-byte payload per row.
def events(from_id, n, batch):
    return spark.range(from_id, from_id + n).select(
        concat(lit("e"), col("id")).alias("id"),
        (col("id") % 1000).alias("user_id"),
        expr("timestamp_millis(1767225600000 + id * 1000)").alias("event_time"),
        when(col("id") % 2 == 0, "2026-01-01").otherwise("2026-01-02").alias("datestr"),
        lit(batch).cast("long").alias("ts"),
        expr("repeat('x', 200)").alias("payload"))


# File groups per partition: a base file is <fileId>_<writeToken>_<instant>.parquet, and every
# version of a file group shares the fileId.
def file_groups(table_path):
    return {p: len({f.split("_")[0] for f in os.listdir(os.path.join(table_path, p)) if f.endswith(".parquet")})
            for p in sorted(os.listdir(table_path))
            if os.path.isdir(os.path.join(table_path, p)) and not p.startswith(".")}


common_opts = {
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.partitionpath.field": "datestr",
    "hoodie.table.ordering.fields": "ts",
}

# The book's sizing options. Values are numbers of bytes.
sizing_opts = {
    **common_opts,
    "hoodie.table.name": "events",
    "hoodie.parquet.max.file.size": "125829120",               # ~120 MB
    "hoodie.parquet.small.file.limit": "104857600",            # ~100 MB
    "hoodie.copyonwrite.record.size.estimate": "1024",         # better first commit
    "hoodie.datasource.write.operation": "insert",
}

sized_path = f"{base_path}/events"
print("=== insert with write-time file sizing ===")
for batch in range(1, 6):
    events((batch - 1) * 20000, 20000, batch).write.format("hudi").options(**sizing_opts).mode("append").save(sized_path)
    print(f"after commit {batch}, file groups per partition: {file_groups(sized_path)}")

bulk_path = f"{base_path}/events_bulk"
bulk_opts = {
    **common_opts,
    "hoodie.table.name": "events_bulk",
    "hoodie.datasource.write.operation": "bulk_insert",
}
print("\n=== bulk_insert, which skips write-time file sizing ===")
for batch in range(1, 6):
    events((batch - 1) * 20000, 20000, batch).write.format("hudi").options(**bulk_opts).mode("append").save(bulk_path)
    print(f"after commit {batch}, file groups per partition: {file_groups(bulk_path)}")

print(f"\nRows: insert table {spark.read.format('hudi').load(sized_path).count()}, "
      f"bulk_insert table {spark.read.format('hudi').load(bulk_path).count()}")
