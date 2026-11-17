"""
Compares one vendor's rows in the raw or silver table as of an instant with the current rows.
PySpark version of src/main/scala/.../governance/TimeTravelExamples.scala.

  ./scripts/run_time_travel_pyspark.sh silver <INSTANT_TIME> V-101
  # or: spark-submit <options> pyspark/time_travel_examples.py <raw|silver> <instant_time> [vendor_id]
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder.appName("Chapter06TimeTravelExamples").getOrCreate()

args = sys.argv[1:]
table_type = args[0] if len(args) > 0 else "raw"              # raw | silver
instant = args[1] if len(args) > 1 else ""
vendor_id = args[2] if len(args) > 2 else "V-101"

if not instant:
    raise ValueError("Usage: time_travel_examples.py <raw|silver> <instant_time> [vendor_id]")

paths = {"raw": "/tmp/hudi/raw/payments", "silver": "/tmp/hudi/silver/vendor_payouts"}
sort_columns = {"raw": "event_ts", "silver": "payout_date"}
if table_type not in paths:
    raise ValueError(f"Unsupported table type '{table_type}'. Expected 'raw' or 'silver'.")
path = paths[table_type]

historical_df = spark.read.format("hudi").option("as.of.instant", instant).load(path)
current_df = spark.read.format("hudi").load(path)

historical_count = historical_df.filter(col("vendor_id") == vendor_id).count()
current_count = current_df.filter(col("vendor_id") == vendor_id).count()

print("==================================================")
print(f"Table type         : {table_type}")
print(f"Path               : {path}")
print(f"Vendor             : {vendor_id}")
print(f"Historical instant : {instant}")
print(f"Historical count   : {historical_count}")
print(f"Current count      : {current_count}")
print("==================================================")

print(f"Historical rows for vendor_id={vendor_id}")
(historical_df
    .filter(col("vendor_id") == vendor_id)
    .orderBy(desc(sort_columns[table_type]))
    .show(50, truncate=False))

print(f"Current rows for vendor_id={vendor_id}")
(current_df
    .filter(col("vendor_id") == vendor_id)
    .orderBy(desc(sort_columns[table_type]))
    .show(50, truncate=False))
