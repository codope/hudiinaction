# Chapter 9, "Choosing where the lock lives" - does StorageBasedLockProvider work on a local
# file:// table path? PySpark version of spark/lock_audit.scala.
#
# StorageSchemes.FILE carries no storage lock class (S3, GCS and Azure do), so the provider has
# no lock client for a local table. On Hudi 1.2.1, with the scheme-less local path used here, the
# write fails while the provider initializes, with "Unsupported scheme :null" at the bottom of the
# exception chain. This script confirms by an actual run that the provider cannot be used locally.

import os
import shutil

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/lock_audit")
table_path = f"{base_path}/rides"
shutil.rmtree(base_path, ignore_errors=True)

schema = "id string, city string, fare double"
(spark.createDataFrame([("r1", "NYC", 12.0)], schema).write.format("hudi")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", "rides")
    .mode("overwrite")
    .save(table_path))
print("Seed write succeeded.")

print("\n=== Attempting to write with hoodie.write.lock.provider=StorageBasedLockProvider on a local file:// path ===")
try:
    (spark.createDataFrame([("r2", "SF", 20.0)], schema).write.format("hudi")
        .option("hoodie.datasource.write.operation", "upsert")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.table.ordering.fields", "id")
        .option("hoodie.table.name", "rides")
        .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.StorageBasedLockProvider")
        .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
        .mode("append")
        .save(table_path))
    print("Write with StorageBasedLockProvider on local file:// SUCCEEDED (unexpected per source reading).")
except Exception as e:
    print("Write FAILED as expected.")
    j = getattr(e, "java_exception", None) or getattr(e, "_origin", None)
    print(f"Exception class:   {j.getClass().getName()}")
    print(f"Exception message: {j.getMessage()}")
    j = j.getCause()
    while j is not None:
        print(f"  caused by: {j.getClass().getName()}: {j.getMessage()}")
        j = j.getCause()

print("LOCK_AUDIT_DONE")
