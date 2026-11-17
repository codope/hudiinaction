# Chapter 5, "Configuring compaction with Spark datasource writer" - inline compaction on a
# Merge-on-Read table with the NUM_OR_TIME trigger: compact after 10 delta commits or 30
# minutes, whichever comes first. PySpark version of spark/02_inline_compaction.scala.
#
# One insert and ten upserts of the same keys. The upserts go to log files; the 10th delta
# commit triggers compaction inside the write, which shows up on the timeline as a `commit`
# action and as new base files.

import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, expr, lit, max as max_, when

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter05/inline_compaction")
table_path = f"{base_path}/events_mor"
shutil.rmtree(base_path, ignore_errors=True)


def events(from_id, n, batch):
    return spark.range(from_id, from_id + n).select(
        concat(lit("e"), col("id")).alias("id"),
        (col("id") % 1000).alias("user_id"),
        expr("timestamp_millis(1767225600000 + id * 1000)").alias("event_time"),
        when(col("id") % 2 == 0, "2026-01-01").otherwise("2026-01-02").alias("datestr"),
        lit(batch).cast("long").alias("ts"),
        expr("repeat('x', 200)").alias("payload"))


# Actions of the completed instants, oldest first. show_timeline lists the newest first.
def completed_actions(path):
    rows = spark.sql(f"CALL show_timeline(path => '{path}', limit => 1000)").collect()
    return [r["action"] for r in sorted(rows, key=lambda r: r["instant_time"]) if r["state"] == "COMPLETED"]


write_opts = {
    "hoodie.table.name": "events_mor",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.partitionpath.field": "datestr",
    "hoodie.table.ordering.fields": "ts",
    # Inline compaction + hybrid trigger
    "hoodie.compact.inline": "true",
    "hoodie.compact.inline.trigger.strategy": "NUM_OR_TIME",
    "hoodie.compact.inline.max.delta.commits": "10",
    "hoodie.compact.inline.max.delta.seconds": "1800",
    # Optional: cap IO if you must stay under a latency budget
    "hoodie.compaction.target.io": "20480",                    # MB, so 20 GB
}


def write(df, operation):
    df.write.format("hudi").options(**write_opts).option("hoodie.datasource.write.operation", operation) \
        .mode("append").save(table_path)


write(events(0, 20000, 1), "insert")

for batch in range(2, 12):
    write(events(0, 2000, batch), "upsert")
    actions = completed_actions(table_path)
    print(f"after write {batch:2d}: {actions.count('deltacommit'):2d} delta commits, "
          f"{actions.count('commit')} compaction(s)")

print("\nCompleted instants on the active timeline:")
for action in completed_actions(table_path):
    print(f"  {action}")

# Read-optimized queries read only base files: they show the upserts merged by the compaction,
# but not the ones written to log files after it. Snapshot queries show all of them.
ro = spark.read.format("hudi").option("hoodie.datasource.query.type", "read_optimized").load(table_path)
snapshot = spark.read.format("hudi").load(table_path)
print(f"\nLatest batch visible: read-optimized ts = {ro.agg(max_('ts')).first()[0]}, "
      f"snapshot ts = {snapshot.agg(max_('ts')).first()[0]}")
