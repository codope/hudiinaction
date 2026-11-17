# Chapter 9, "Validating data before it commits" - pre-commit SQL validators: a passing
# validation, a failing one, the exception it raises, and what the timeline looks like
# afterward. Also exercises hoodie.precommit.validators.failure.policy (FAIL vs WARN_LOG).
# PySpark version of spark/validators.scala.
#
# Run it with `scripts/run_pyspark.sh validators`, which puts the Hudi bundle on
# --driver-class-path: the validators run on the JVM's common ForkJoinPool, whose threads
# cannot see jars added with --packages alone (see the README's environment notes).

import os
import shutil

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/validators")
shutil.rmtree(base_path, ignore_errors=True)


def timeline_files(table_path):
    d = f"{table_path}/.hoodie/timeline"
    return sorted(f for f in os.listdir(d) if not f.endswith(".crc")) if os.path.exists(d) else []


# Class and message of the JVM exception behind a PySpark error, then its causes.
def java_error_chain(e):
    j = getattr(e, "java_exception", None) or getattr(e, "_origin", None)
    if j is None:
        return [f"{type(e).__name__}: {e}"]
    chain = []
    while j is not None:
        chain.append((j.getClass().getName(), j.getMessage()))
        j = j.getCause()
    return chain


def rides(rows):
    return spark.createDataFrame(rows, "id string, city string, fare double")


def seed(table_path):
    (rides([("r1", "NYC", 12.0), ("r2", "NYC", 14.5), ("r3", "SF", 22.0), ("r4", "SF", 9.0), ("r5", "NYC", 31.0)])
        .write.format("hudi")
        .option("hoodie.datasource.write.operation", "bulk_insert")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.table.ordering.fields", "id")
        .option("hoodie.table.name", os.path.basename(table_path))
        .mode("overwrite")
        .save(table_path))


# ============================================================================
# 1. PASSING validation: SqlQueryEqualityPreCommitValidator, an invariant unaffected
#    by the write (the set of distinct cities), plus SqlQuerySingleResultPreCommitValidator
#    checking the exact row count after the write.
# ============================================================================
pass_table = f"{base_path}/pass"
seed(pass_table)
print("\n=== PASS scenario: insert 2 more rows, no new city ===")
try:
    (rides([("r6", "NYC", 8.0), ("r7", "SF", 19.0)]).write.format("hudi")
        .option("hoodie.datasource.write.operation", "upsert")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.table.ordering.fields", "id")
        .option("hoodie.table.name", os.path.basename(pass_table))
        .option("hoodie.precommit.validators",
                "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator,"
                "org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator")
        .option("hoodie.precommit.validators.equality.sql.queries",
                "select distinct city from <TABLE_NAME> order by city")
        .option("hoodie.precommit.validators.single.value.sql.queries",
                "select count(*) from <TABLE_NAME>#7")
        .mode("append")
        .save(pass_table))
    print("Commit SUCCEEDED: both validators passed (city set unchanged; row count = 7 as expected).")
except Exception as e:
    print("Commit FAILED unexpectedly: {}: {}".format(*java_error_chain(e)[0]))
print("Timeline after PASS scenario: " + ", ".join(timeline_files(pass_table)))

# ============================================================================
# 2. FAILING validation: the same equality validator, but this write DELETES every SF row.
#
# SqlQueryEqualityPreCommitValidator does NOT check symmetric equality of the before/after
# query results. Its check is
#   prevRows.intersect(newRows).count() == prevRows.count()
# i.e. "every row present BEFORE must still be present AFTER" -- a one-directional subset
# check. Adding a brand-new city does NOT fail it. The only way to fail it is to make something
# from the BEFORE result disappear, which is what this scenario does: it deletes every SF row,
# so `select distinct city` goes from {NYC, SF} to {NYC} and the subset check breaks.
# ============================================================================
fail_table = f"{base_path}/fail"
seed(fail_table)
print("\n=== FAIL scenario: DELETE every SF row (makes a previously-present city vanish) ===")
print("Timeline before the failing write: " + ", ".join(timeline_files(fail_table)))
delete_sf_rows = rides([("r3", "SF", 22.0), ("r4", "SF", 9.0)])
try:
    (delete_sf_rows.write.format("hudi")
        .option("hoodie.datasource.write.operation", "delete")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.table.ordering.fields", "id")
        .option("hoodie.table.name", os.path.basename(fail_table))
        .option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator")
        .option("hoodie.precommit.validators.equality.sql.queries",
                "select distinct city from <TABLE_NAME> order by city")
        .mode("append")
        .save(fail_table))
    print("Commit SUCCEEDED (unexpected -- validator should have failed).")
except Exception as e:
    chain = java_error_chain(e)
    print("Commit FAILED as expected.")
    print(f"Exception class:   {chain[0][0]}")
    print(f"Exception message: {chain[0][1]}")
    for cls, msg in chain[1:]:
        print(f"  caused by: {cls}: {msg}")
print("Timeline after the failing write: " + ", ".join(timeline_files(fail_table)))
print("Row count after the failed write (should still be 5, the seed count): " +
      str(spark.read.format("hudi").load(fail_table).count()))

# ============================================================================
# 3. hoodie.precommit.validators.failure.policy=WARN_LOG
#    Same failing validator, configured to warn instead of blocking the commit. In Hudi 1.2.x
#    only the streaming offset validators read this policy (StreamingOffsetValidator; Flink's
#    FlinkKafkaOffsetValidator); SparkValidatorUtils fails the commit whenever a SQL validator
#    fails, so this delete is still blocked and the table keeps its 5 rows.
# ============================================================================
warn_table = f"{base_path}/warn_policy"
seed(warn_table)
print("\n=== WARN_LOG policy scenario: same failing (SF-deleting) validator, but failure.policy=WARN_LOG ===")
try:
    (delete_sf_rows.write.format("hudi")
        .option("hoodie.datasource.write.operation", "delete")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.table.ordering.fields", "id")
        .option("hoodie.table.name", os.path.basename(warn_table))
        .option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator")
        .option("hoodie.precommit.validators.equality.sql.queries",
                "select distinct city from <TABLE_NAME> order by city")
        .option("hoodie.precommit.validators.failure.policy", "WARN_LOG")
        .mode("append")
        .save(warn_table))
    print("Commit SUCCEEDED under WARN_LOG policy (validator failure only logged).")
except Exception as e:
    print("Commit FAILED even under WARN_LOG: {}: {}".format(*java_error_chain(e)[0]))
print("Row count under WARN_LOG policy (5 if the SQL validator still blocked the delete, 3 if it went through): " +
      str(spark.read.format("hudi").load(warn_table).count()))

# The failed commits above leave idle, non-daemon executor threads behind in Hudi 1.2.1, which
# keep the JVM running after this script ends. Stop Spark and exit the JVM explicitly, as the
# Scala version does with System.exit(0).
spark.stop()
try:
    spark.sparkContext._jvm.System.exit(0)
except Exception:
    pass   # the JVM has exited, so the call never returns
