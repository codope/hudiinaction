"""
GDPR delete of one vendor's rows from the raw and silver tables.
PySpark version of src/main/scala/.../governance/GdprDelete.scala.

  ./scripts/run_gdpr_delete_pyspark.sh V-101
  # or: spark-submit <options> pyspark/gdpr_delete.py [vendor_id] [raw_path] [silver_path]
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Chapter06GdprDelete").getOrCreate()

args = sys.argv[1:]
vendor_id = args[0] if len(args) > 0 else "V-100"
raw_path = args[1] if len(args) > 1 else "/tmp/hudi/raw/payments"
silver_path = args[2] if len(args) > 2 else "/tmp/hudi/silver/vendor_payouts"

print(f"Starting GDPR delete for vendor_id={vendor_id}")


def vendor_rows(path):
    return spark.read.format("hudi").load(path).filter(col("vendor_id") == vendor_id)


raw_before = vendor_rows(raw_path).count()
silver_before = vendor_rows(silver_path).count()
print(f"Rows before delete -> raw={raw_before}, silver={silver_before}")

raw_to_delete = vendor_rows(raw_path).select("event_id", "dt", "event_ts")

if raw_to_delete.head(1):
    (raw_to_delete.write
        .format("hudi")
        .option("hoodie.table.name", "hudi_payments_raw")
        .option("hoodie.datasource.write.recordkey.field", "event_id")
        .option("hoodie.datasource.write.precombine.field", "event_ts")
        .option("hoodie.datasource.write.partitionpath.field", "dt")
        .option("hoodie.datasource.write.operation", "delete")
        .mode("append")
        .save(raw_path))
else:
    print(f"No raw rows found for vendor_id={vendor_id}")

silver_to_delete = vendor_rows(silver_path).select("vendor_id", "order_line_id", "payout_date", "line_version")

if silver_to_delete.head(1):
    (silver_to_delete.write
        .format("hudi")
        .option("hoodie.table.name", "hudi_vendor_payouts")
        .option("hoodie.datasource.write.recordkey.field", "vendor_id,order_line_id")
        .option("hoodie.datasource.write.precombine.field", "line_version")
        .option("hoodie.datasource.write.partitionpath.field", "payout_date")
        .option("hoodie.index.type", "GLOBAL_RECORD_LEVEL_INDEX")
        .option("hoodie.datasource.write.operation", "delete")
        .mode("append")
        .save(silver_path))
else:
    print(f"No silver rows found for vendor_id={vendor_id}")

raw_after = vendor_rows(raw_path).count()
silver_after = vendor_rows(silver_path).count()

print(f"Rows after delete  -> raw={raw_after}, silver={silver_after}")
print(f"GDPR delete complete for vendor_id={vendor_id}")
