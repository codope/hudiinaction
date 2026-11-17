# Chapter 9, "Storing semi-structured and unstructured data" - reproduces the documented
# Spark 4.1 + MOR VARIANT read issue from the Hudi 1.2 release notes' Known Issues section
# (spark.sql.variant.pushVariantIntoScan, on by default on Spark 4.1, off by default on
# Spark 4.0): "A top-level VARIANT value set to null by an update is read back as a struct
# of nulls instead of NULL, so a filter such as `v IS NULL` misses the row." A fix is
# tracked in https://github.com/apache/hudi/pull/20039; the corroborating GitHub issue is
# https://github.com/apache/hudi/issues/20037. PySpark version of spark/variant_pushdown_mor.scala.
#
# Run variant_events.py first: this reads the agent_events_mor table it leaves behind, where
# the upsert set e3's payload to NULL and the update landed in an uncompacted .log file (MOR),
# which is the shape the known issue requires ("log files are merged"). Requires Spark 4.1.x
# specifically: the flag defaults to false on Spark 4.0.

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("VARIANT_BASE", "/tmp/hudiinaction/chapter09")
mor_path = f"{base_path}/variant_mor/agent_events_mor"

print(f"spark.version = {spark.version}")


def read_mor_snapshot():
    return (spark.read.format("hudi").load(mor_path)
            .selectExpr("event_id", "payload IS NULL AS payload_is_null", "CAST(payload AS STRING) AS payload_str")
            .orderBy("event_id"))


print("=== pushVariantIntoScan=true (Spark 4.1 default): snapshot read of MOR table with an uncompacted update ===")
spark.conf.set("spark.sql.variant.pushVariantIntoScan", "true")
read_mor_snapshot().show(truncate=False)
print("Rows where payload_is_null=true and `IS NULL` correctly caught the nulled-out e3 row (if any):")
spark.read.format("hudi").load(mor_path).where("payload IS NULL").select("event_id").show(truncate=False)

print("=== workaround: pushVariantIntoScan=false ===")
spark.conf.set("spark.sql.variant.pushVariantIntoScan", "false")
read_mor_snapshot().show(truncate=False)
print("Rows caught by `payload IS NULL` with the workaround applied:")
spark.read.format("hudi").load(mor_path).where("payload IS NULL").select("event_id").show(truncate=False)

print("VARIANT_PUSHDOWN_MOR_DONE")
