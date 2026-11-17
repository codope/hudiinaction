# Chapter 9, "Looking up records by a non-key column" / "Indexing a derived value" - the
# chapter's exact queries, on a table named `rides` with a `ride_id` column (matching the
# chapter's prose exactly). PySpark version of spark/index_pruning_rides.scala.
#
# The Scala version reads the scan node's metrics from the executed plan. PySpark does not
# expose the plan's metrics, so this version reads them where the chapter tells readers to
# look: the SQL tab of the Spark UI, through its REST API (/api/v1/applications/<id>/sql).
# The number to compare is the scan node's "number of files read".

import json
import os
import shutil
import time
from urllib.request import urlopen

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/index_pruning_rides")
table_path = f"{base_path}/rides"
table_name = "rides"

shutil.rmtree(base_path, ignore_errors=True)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")


class JavaRandom:
    """java.util.Random (which scala.util.Random wraps), so the generated rides, and with them
    the file counts the chapter prints, match the Scala run."""

    def __init__(self, seed):
        self.seed = (seed ^ 0x5DEECE66D) & ((1 << 48) - 1)

    def _next(self, bits):
        self.seed = (self.seed * 0x5DEECE66D + 0xB) & ((1 << 48) - 1)
        return self.seed >> (48 - bits)

    def next_int(self, bound):
        if bound & -bound == bound:
            return (bound * self._next(31)) >> 31
        while True:
            bits = self._next(31)
            val = bits % bound
            if bits - val + (bound - 1) < (1 << 31):
                return val

    def next_double(self):
        return ((self._next(26) << 27) + self._next(27)) / float(1 << 53)


spark.sql(f"""
  CREATE TABLE {table_name} (
      ts       BIGINT,
      ride_id  STRING,
      rider    STRING,
      driver   STRING,
      fare     DOUBLE,
      city     STRING
  ) USING hudi
  OPTIONS (
      primaryKey = 'ride_id',
      hoodie.metadata.record.index.enable = 'true',
      hoodie.metadata.index.column.stats.enable = 'true',
      hoodie.metadata.index.column.stats.column.list = 'driver',
      hoodie.write.record.merge.mode = 'COMMIT_TIME_ORDERING',
      hoodie.parquet.small.file.limit = '0',
      hoodie.copyonwrite.record.size.estimate = '64',
      hoodie.parquet.max.file.size = '10240'
  )
  PARTITIONED BY (city)
  LOCATION '{table_path}'
""")

cities = ["san_francisco"]
riders = [f"rider-{i:03d}" for i in range(1, 51)]
drivers = [f"driver-{i:03d}" for i in range(1, 51)]
rnd = JavaRandom(7)

batches = 25
per_batch = 40
for b in range(batches):
    rows = []
    for i in range(per_batch):
        city = cities[0]
        rider = riders[rnd.next_int(len(riders))]
        driver = drivers[rnd.next_int(len(drivers))]
        fare = 5.0 + rnd.next_double() * 95.0
        ts = 1700000000 + b * 100000 + i
        rows.append((ts, f"trip-{b}-{i}", rider, driver, fare, city))
    (spark.createDataFrame(rows, "ts bigint, ride_id string, rider string, driver string, fare double, city string")
        .write.format("hudi")
        .option("hoodie.datasource.write.operation", "insert")
        .option("hoodie.parquet.small.file.limit", "0")
        .option("hoodie.copyonwrite.insert.split.size", str(per_batch))
        .mode("append")
        .save(table_path))
spark.sql(f"REFRESH TABLE {table_name}")
total_files = spark.sql(f"SELECT count(distinct _hoodie_file_name) as f FROM {table_name}").first()["f"]
print(f"Loaded {spark.table(table_name).count()} rows into {table_name} across {total_files} base files (single partition)")

# The chapter queries a fixed date. Batch 10 starts at 2023-11-26 12:00 UTC, so its 40 rows fall on
# that calendar day in every session time zone between UTC-11 and UTC+11.
a_date_present = "2023-11-26"
print(f"Date used in the chapter's exact query below: {a_date_present}")

spark.sql("SET hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider")
spark.sql(f"CREATE INDEX record_index ON {table_name} (ride_id)")
spark.sql(f"CREATE INDEX idx_driver ON {table_name} (driver)")
spark.sql(f"""
  CREATE INDEX idx_ts_date ON {table_name} USING column_stats(ts)
  OPTIONS(expr='from_unixtime', format='yyyy-MM-dd')
""")


def sql_executions():
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/sql?details=true&planDescription=false&length=100000"
    return json.load(urlopen(url))


def print_scan_metrics(run):
    """Runs `run`, then prints the metrics of the scan node of the last query it executed,
    as the SQL tab of the Spark UI shows them."""
    seen = max((e["id"] for e in sql_executions()), default=-1)
    run()
    for _ in range(100):   # the UI's status store is updated asynchronously
        scans = [(e["id"], node) for e in sql_executions() if e["id"] > seen and e["status"] == "COMPLETED"
                 for node in e["nodes"] if node["nodeName"].startswith("Scan")]
        if scans:
            break
        time.sleep(0.1)
    last_id = max(i for i, _ in scans)
    for i, node in scans:
        if i == last_id:
            print(f"  scan node: {node['nodeName']}")
            for m in sorted(node["metrics"], key=lambda m: m["name"]):
                print(f"    {m['name']} = {m['value']}")


def run_with_skipping(label, enabled, select_cols, filter_expr, count_only):
    spark.catalog.clearCache()
    print(f"\n--- {label} (hoodie.enable.data.skipping={str(enabled).lower()}, hoodie.metadata.enable={str(enabled).lower()}) ---")
    reader = (spark.read.format("hudi")
              .option("hoodie.enable.data.skipping", str(enabled).lower())
              .option("hoodie.metadata.enable", str(enabled).lower()))
    filtered = reader.load(table_path).select(*select_cols).where(filter_expr)
    if count_only:
        print(f"count(*) = {filtered.count()}")
    else:
        filtered.show(truncate=False)
    spark.catalog.clearCache()
    print_scan_metrics(lambda: reader.load(table_path).select(*select_cols).where(filter_expr).collect())


print("\n############################################################")
print("# CHAPTER'S EXACT QUERY 1: SELECT ride_id, fare FROM rides WHERE driver = 'driver-001'")
print("############################################################")
run_with_skipping("Data skipping ON", True, ["ride_id", "fare"], "driver = 'driver-001'", False)
run_with_skipping("Data skipping OFF", False, ["ride_id", "fare"], "driver = 'driver-001'", False)

print("\n############################################################")
print(f"# CHAPTER'S EXACT QUERY 2: SELECT count(*) FROM rides WHERE from_unixtime(ts, 'yyyy-MM-dd') = '{a_date_present}'")
print("############################################################")
run_with_skipping("Data skipping ON", True, ["ride_id", "ts"], f"from_unixtime(ts, 'yyyy-MM-dd') = '{a_date_present}'", True)
run_with_skipping("Data skipping OFF", False, ["ride_id", "ts"], f"from_unixtime(ts, 'yyyy-MM-dd') = '{a_date_present}'", True)

spark.sql("SET hoodie.enable.data.skipping=true")
spark.sql("SET hoodie.metadata.enable=true")

print("INDEX_PRUNING_RIDES_DONE")
