# Chapter 5, "Configuring Inline Clustering" - inline clustering every 4 commits, which bin-packs
# small files toward ~1 GiB and sorts the rewritten files by event_time and user_id.
# PySpark version of spark/05_inline_clustering.scala.
#
# To get small files to cluster, this example turns off write-time small-file handling
# (hoodie.parquet.small.file.limit=0), so each insert adds a new file group per partition.
# The 4th commit then runs clustering, which replaces them with one sorted file group per
# partition and lands on the timeline as a replacecommit.

import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, countDistinct, expr, lit, rand, when

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter05/inline_clustering")
table_path = f"{base_path}/events_clustered"
shutil.rmtree(base_path, ignore_errors=True)


# Rows arrive in random order, so the sort done by clustering is visible.
def events(from_id, n, batch):
    return spark.range(from_id, from_id + n).select(
        concat(lit("e"), col("id")).alias("id"),
        (rand(batch) * 1000).cast("long").alias("user_id"),
        expr("timestamp_millis(1767225600000 + cast(rand() * 86400000 as long))").alias("event_time"),
        when(col("id") % 2 == 0, "2026-01-01").otherwise("2026-01-02").alias("datestr"),
        lit(batch).cast("long").alias("ts"),
        expr("repeat('x', 200)").alias("payload"))


# File groups in the latest snapshot, per partition.
def latest_file_groups(path):
    rows = (spark.read.format("hudi").load(path)
            .groupBy("_hoodie_partition_path").agg(countDistinct("_hoodie_file_name").alias("files"))
            .collect())
    return {r[0]: r[1] for r in sorted(rows)}


# Actions of the completed instants, oldest first. show_timeline lists the newest first.
def completed_actions(path):
    rows = spark.sql(f"CALL show_timeline(path => '{path}', limit => 1000)").collect()
    return [r["action"] for r in sorted(rows, key=lambda r: r["instant_time"]) if r["state"] == "COMPLETED"]


write_opts = {
    "hoodie.table.name": "events_clustered",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.partitionpath.field": "datestr",
    "hoodie.table.ordering.fields": "ts",
    "hoodie.datasource.write.operation": "insert",
    "hoodie.parquet.small.file.limit": "0",                    # demo only: produce small files
    # turn on inline (synchronous) clustering; alternatively use async in a companion job
    "hoodie.clustering.inline": "true",
    "hoodie.clustering.inline.max.commits": "4",
    # plan: pick small files and group them to hit the target size
    "hoodie.clustering.plan.strategy.small.file.limit": "629145600",         # ~600 MB
    "hoodie.clustering.plan.strategy.target.file.max.bytes": "1073741824",   # ~1 GiB
    # execution: sort-by hot columns while writing
    "hoodie.clustering.execution.strategy.class":
        "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
    "hoodie.clustering.plan.strategy.sort.columns": "event_time,user_id",
}

for batch in range(1, 5):
    events((batch - 1) * 20000, 20000, batch).write.format("hudi").options(**write_opts).mode("append").save(table_path)
    print(f"after commit {batch}: file groups per partition {latest_file_groups(table_path)}, "
          f"replacecommits {completed_actions(table_path).count('replacecommit')}")

print(f"\nRows after clustering: {spark.read.format('hudi').load(table_path).count()} (4 x 20,000)")
