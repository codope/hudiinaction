# Chapter 5, "Scheduling compaction inline and executing asynchronously" - the writer only
# plans compaction (hoodie.compact.schedule.inline=true); a separate step executes the plan.
# PySpark version of spark/03_schedule_compaction.scala.
#
# After the default trigger of 5 delta commits, the writer leaves a compaction plan in the
# REQUESTED state. Here CALL run_compaction executes it; in production this is a separate job,
# such as HoodieCompactor, which may need a lock provider.
#
# The Spark datasource turns inline compaction on by default for Merge-on-Read batch writes, and
# Hudi rejects inline and schedule-only compaction together, so the writer sets
# hoodie.compact.inline=false explicitly.

import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, expr, lit, when

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter05/schedule_compaction")
table_path = f"{base_path}/events_mor_sched"
shutil.rmtree(base_path, ignore_errors=True)


def events(from_id, n, batch):
    return spark.range(from_id, from_id + n).select(
        concat(lit("e"), col("id")).alias("id"),
        (col("id") % 1000).alias("user_id"),
        expr("timestamp_millis(1767225600000 + id * 1000)").alias("event_time"),
        when(col("id") % 2 == 0, "2026-01-01").otherwise("2026-01-02").alias("datestr"),
        lit(batch).cast("long").alias("ts"),
        expr("repeat('x', 200)").alias("payload"))


# show_compaction lists every compaction instant with its state: REQUESTED, INFLIGHT or COMPLETED.
def compactions(path):
    return spark.sql(f"CALL show_compaction(path => '{path}')").collect()


def pending_compactions(path):
    return [f"{r['timestamp']} ({r['state']})" for r in compactions(path) if r["state"] != "COMPLETED"]


def completed_compactions(path):
    return sum(1 for r in compactions(path) if r["state"] == "COMPLETED")


write_opts = {
    "hoodie.table.name": "events_mor_sched",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.partitionpath.field": "datestr",
    "hoodie.table.ordering.fields": "ts",
    "hoodie.compact.inline": "false",                          # overrides the MoR default
    "hoodie.compact.schedule.inline": "true",                  # plans only
}


def write(df, operation):
    df.write.format("hudi").options(**write_opts).option("hoodie.datasource.write.operation", operation) \
        .mode("append").save(table_path)


write(events(0, 20000, 1), "insert")
for batch in range(2, 7):
    write(events(0, 2000, batch), "upsert")

print(f"Pending compaction plans after 6 writes: {', '.join(pending_compactions(table_path))}")
print(f"Completed compactions: {completed_compactions(table_path)}")

print("\n=== Executing the scheduled plan ===")
spark.sql(f"CALL run_compaction(op => 'run', path => '{table_path}')").show(truncate=False)

print(f"Pending compaction plans: {', '.join(pending_compactions(table_path))}")
print(f"Completed compactions: {completed_compactions(table_path)}")
