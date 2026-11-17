# Chapter 5, "Recovering from failures and disasters" - rolling back a commit, creating a
# savepoint, and restoring the table to it. PySpark version of spark/07_recovery.scala.
#
# The chapter runs these steps in the Hudi CLI (the commands are in cli/hudi_cli_recovery.md).
# This script runs the same steps through Spark SQL procedures, which need no extra install:
#   commit rollback --commit <instant>       ->  CALL rollback_to_instant
#   savepoint create --commit <instant>      ->  CALL create_savepoint
#   savepoints show                          ->  CALL show_savepoints
#   savepoint rollback --savepoint <instant> ->  CALL rollback_to_savepoint
# As the chapter says, pause all writers before a restore.

import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, count, lit, sum as sum_, when

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter05/recovery")
table_path = f"{base_path}/events_recovery"
table_name = "events_recovery"

shutil.rmtree(base_path, ignore_errors=True)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def events(from_id, n, batch, amount):
    return spark.range(from_id, from_id + n).select(
        concat(lit("e"), col("id")).alias("id"),
        when(col("id") % 2 == 0, "2026-01-01").otherwise("2026-01-02").alias("datestr"),
        lit(batch).cast("long").alias("ts"),
        lit(amount).cast("double").alias("amount"))


write_opts = {
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.partitionpath.field": "datestr",
    "hoodie.table.ordering.fields": "ts",
    "hoodie.datasource.write.operation": "upsert",
}


def write(df):
    df.write.format("hudi").options(**write_opts).mode("append").save(table_path)
    return spark.sql(f"CALL show_commits(table => '{table_name}', limit => 1)").first()["commit_time"]


def state(label):
    r = spark.read.format("hudi").load(table_path).agg(count(lit(1)), sum_("amount")).first()
    print(f"{label:<40} rows = {r[0]:5d}, sum(amount) = {r[1]:.1f}")


events(0, 1000, 1, 10.0).write.format("hudi").options(**write_opts).mode("append").save(table_path)
# Procedures look the table up by name, so register it once it exists.
spark.sql(f"CREATE TABLE {table_name} USING hudi LOCATION '{table_path}'")
state("commit 1: initial load")

good_commit = write(events(1000, 1000, 2, 10.0)); state("commit 2: second load")

print(f"\n=== Savepoint at commit 2 ({good_commit}) ===")
spark.sql(f"CALL create_savepoint(table => '{table_name}', commit_time => '{good_commit}')").show(truncate=False)
spark.sql(f"CALL show_savepoints(table => '{table_name}')").show(truncate=False)

bad_commit = write(events(0, 2000, 3, 999.0)); state("commit 3: bad update of every row")
print(f"\n=== Rolling back commit 3 ({bad_commit}) ===")
spark.sql(f"CALL rollback_to_instant(table => '{table_name}', instant_time => '{bad_commit}')").show(truncate=False)
state("after rollback of commit 3")

write(events(2000, 500, 4, 10.0)); state("commit 4: new rows")
write(events(0, 2000, 5, -1.0)); state("commit 5: bad backfill")

print(f"\n=== Restoring to the savepoint at commit 2 ({good_commit}) ===")
spark.sql(f"CALL rollback_to_savepoint(table => '{table_name}', instant_time => '{good_commit}')").show(truncate=False)
state("after restore to savepoint")

# Remove savepoints you no longer need, so cleaning and archival can proceed.
spark.sql(f"CALL delete_savepoint(table => '{table_name}', instant_time => '{good_commit}')").show(truncate=False)
