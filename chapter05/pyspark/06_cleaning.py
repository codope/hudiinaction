# Chapter 5, "Configuring Cleaner policies" - the three cleaner configurations from the chapter.
# PySpark version of spark/06_cleaning.scala.
#
# 1. Inline cleaning, KEEP_LATEST_COMMITS with 10 commits retained. Fifteen upserts rewrite the
#    same file group; the number of retained file versions grows, then stops growing once the
#    cleaner starts removing versions older than the retention window.
# 2. Async cleaning: the same policy, with deletion done in the background.
# 3. Time-based retention: KEEP_LATEST_BY_HOURS with 24 hours.
#
# The chapter calls the async switch hoodie.clean.async and the cadence switch
# hoodie.clean.max.commits; Hudi 1.2.1 still accepts them as aliases of
# hoodie.clean.async.enabled and hoodie.clean.trigger.max.commits.

import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, expr, lit

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter05/cleaning")
shutil.rmtree(base_path, ignore_errors=True)


def events(from_id, n, batch):
    return spark.range(from_id, from_id + n).select(
        concat(lit("e"), col("id")).alias("id"),
        (col("id") % 1000).alias("user_id"),
        expr("timestamp_millis(1767225600000 + id * 1000)").alias("event_time"),
        lit("2026-01-01").alias("datestr"),
        lit(batch).cast("long").alias("ts"),
        expr("repeat('x', 200)").alias("payload"))


def base_file_versions(table_path):
    return sum(1 for f in os.listdir(f"{table_path}/2026-01-01") if f.endswith(".parquet"))


def completed_cleans(path):
    rows = spark.sql(f"CALL show_timeline(path => '{path}', limit => 1000)").collect()
    return sum(1 for r in rows if r["action"] == "clean" and r["state"] == "COMPLETED")


common_opts = {
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.partitionpath.field": "datestr",
    "hoodie.table.ordering.fields": "ts",
    "hoodie.datasource.write.operation": "upsert",
}

# 1. Inline cleaning (Spark)
inline_clean = {
    "hoodie.clean.automatic": "true",
    "hoodie.clean.policy": "KEEP_LATEST_COMMITS",
    "hoodie.clean.commits.retained": "10",                     # keep last 10 commits
}

# 2. Async cleaning
async_clean = {
    "hoodie.clean.automatic": "true",
    "hoodie.clean.async": "true",
}

# 3. Time-based retention
by_hours = {
    "hoodie.clean.policy": "KEEP_LATEST_BY_HOURS",
    "hoodie.clean.hours.retained": "24",
}

inline_path = f"{base_path}/events_clean_inline"
print("=== KEEP_LATEST_COMMITS, 10 commits retained ===")
for batch in range(1, 16):
    (events(0, 5000, batch).write.format("hudi").options(**common_opts, **inline_clean)
        .option("hoodie.table.name", "events_clean_inline").mode("append").save(inline_path))
    print(f"after commit {batch:2d}: {base_file_versions(inline_path):2d} base file versions, "
          f"{completed_cleans(inline_path)} clean(s)")

# The other two configurations, each on its own table. The async cleaner starts at the beginning
# of each write, so it runs one commit behind the inline cleaner. Time-based retention keeps every
# version written in the last 24 hours, so nothing is cleaned in this short run.
for name, opts in [("events_clean_async", {**inline_clean, **async_clean}), ("events_clean_hours", by_hours)]:
    path = f"{base_path}/{name}"
    for batch in range(1, 16):
        (events(0, 5000, batch).write.format("hudi").options(**common_opts, **opts)
            .option("hoodie.table.name", name).mode("append").save(path))
    print(f"\n{name} after 15 commits: {base_file_versions(path)} base file versions, {completed_cleans(path)} clean(s)")
