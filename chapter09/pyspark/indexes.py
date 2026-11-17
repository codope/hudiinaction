# Chapter 9, "Finding records by key, by value, and by similarity" - SQL index management
# on a table large enough to spread across several files: CREATE INDEX for a secondary
# index, an expression index, a column-stats index, and a bloom-filter index; SHOW INDEXES;
# REFRESH INDEX; DROP INDEX. Shows evidence the indexes are used (metadata table partitions,
# EXPLAIN, a secondary-index lookup). PySpark version of spark/indexes.scala.
#
# A few thousand synthetic rides, generated here, spread over several partitions and small
# files so file-level pruning is visible.

import os
import shutil
import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# The innermost JVM exception behind a PySpark error, as "class: message".
def root_cause(e):
    j = getattr(e, "java_exception", None) or getattr(e, "_origin", None)
    if j is None:
        return f"{type(e).__name__}: {e}"
    while j.getCause() is not None:
        j = j.getCause()
    return f"{j.getClass().getName()}: {j.getMessage()}"

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/indexes")
table_path = f"{base_path}/rides"
table_name = "rides"

shutil.rmtree(base_path, ignore_errors=True)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")


class JavaRandom:
    """java.util.Random (which scala.util.Random wraps), so the generated rides match the Scala run."""

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


# ============================================================================
# 1. A table with a few thousand rows across several partitions and small files
# ============================================================================
# column stats on `driver` + partition stats are enabled at create time. The record index
# flag below is not enough on its own: the inserts use df.write, which ignores the table's
# OPTIONS, so the record index is built explicitly with CREATE INDEX in section 2.
# secondary index requires COMMIT_TIME_ORDERING (see sql_ddl.md's "Create Secondary Index").
spark.sql(f"""
  CREATE TABLE {table_name} (
      ts       BIGINT,
      id       STRING,
      rider    STRING,
      driver   STRING,
      fare     DOUBLE,
      city     STRING
  ) USING hudi
  OPTIONS (
      primaryKey = 'id',
      hoodie.metadata.record.index.enable = 'true',
      hoodie.metadata.index.column.stats.enable = 'true',
      hoodie.metadata.index.column.stats.column.list = 'driver',
      hoodie.write.record.merge.mode = 'COMMIT_TIME_ORDERING',
      hoodie.parquet.small.file.limit = '0',
      hoodie.copyonwrite.record.size.estimate = '256'
  )
  PARTITIONED BY (city)
  LOCATION '{table_path}'
""")

cities = ["san_francisco", "sunnyvale", "austin", "houston", "denver"]
riders = [f"rider-{i:03d}" for i in range(1, 51)]
drivers = [f"driver-{i:03d}" for i in range(1, 51)]
n = 3000
rnd = JavaRandom(42)

# Several small insert batches per partition, so each partition ends up with more than
# one base file -- otherwise "files scanned" pruning is trivially 1 regardless of indexes.
batches = 10
for b in range(batches):
    rows = []
    for i in range(n // batches):
        city = cities[rnd.next_int(len(cities))]
        rider = riders[rnd.next_int(len(riders))]
        driver = drivers[rnd.next_int(len(drivers))]
        fare = 5.0 + rnd.next_double() * 95.0
        ts = 1700000000 + b * 100000 + i
        rows.append((ts, f"trip-{b}-{i}", rider, driver, fare, city))
    (spark.createDataFrame(rows, "ts bigint, id string, rider string, driver string, fare double, city string")
        .write.format("hudi")
        .option("hoodie.datasource.write.operation", "insert")
        .mode("append")
        .save(table_path))
spark.sql(f"REFRESH TABLE {table_name}")
print(f"Loaded {spark.table(table_name).count()} rows into {table_name} across {len(cities)} partitions, {batches} insert batches")

print("\n=== Files per partition before any index-driven pruning ===")
spark.sql(f"SELECT city, count(distinct _hoodie_file_name) as files, count(*) as rows FROM {table_name} "
          "GROUP BY city ORDER BY city").show(truncate=False)

# ============================================================================
# 2. CREATE INDEX: secondary index, expression index, column-stats index, bloom-filter index
# ============================================================================
# CREATE INDEX builds asynchronously and needs a lock provider even for a single writer.
# The lock provider is a writer-side setting, not part of the table config persisted from the
# CREATE TABLE OPTIONS above, so it is set in the session before running CREATE INDEX.
# InProcessLockProvider is enough since everything here runs in this one Spark application.
spark.sql("SET hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider")

# A secondary index resolves record keys through the record index, so build that first.
print("\n=== CREATE INDEX: record index on the record key `id` ===")
spark.sql(f"CREATE INDEX record_index ON {table_name} (id)")

print("\n=== CREATE INDEX: secondary index on `driver` ===")
spark.sql(f"CREATE INDEX idx_driver ON {table_name} (driver)")

print("\n=== CREATE INDEX: expression index on from_unixtime(ts, 'yyyy-MM-dd') ===")
# ts is a raw epoch-seconds BIGINT here, so drive the expression off from_unixtime(ts, fmt),
# matching the from_unixtime example in sql_ddl.md.
spark.sql(f"""
  CREATE INDEX idx_ts_date ON {table_name} USING column_stats(ts)
  OPTIONS(expr='from_unixtime', format='yyyy-MM-dd')
""")

print("\n=== CHAPTER'S SHOW INDEXES, for exactly idx_driver + idx_ts_date + the config-enabled indexes (before any other CREATE INDEX) ===")
spark.sql(f"SHOW INDEXES FROM {table_name}").show(truncate=False)

print("\n=== CREATE INDEX: column-stats index on `rider` (via SQL) ===")
# A bare, untransformed column_stats index cannot be created through CREATE INDEX: with
# no expr (or expr='identity'), CreateIndexCommand refuses with HoodieIndexException
# ("Column stats index without expression on any column can be created using datasource
# configs"). That path is what `hoodie.metadata.index.column.stats.column.list` in the
# CREATE TABLE OPTIONS above already used for `driver`. CREATE INDEX ... USING
# column_stats(...) always needs a real transform, which makes it an expression index in
# practice -- so this demonstrates a second expression index, on lower(rider).
spark.sql(f"CREATE INDEX idx_rider_lower ON {table_name} USING column_stats(rider) OPTIONS(expr='lower')")

print("\n=== CREATE INDEX: bloom-filter index on lower(rider) ===")
spark.sql(f"CREATE INDEX idx_bloom_rider ON {table_name} USING bloom_filters(rider) OPTIONS(expr='lower')")

# ============================================================================
# 3. SHOW INDEXES / REFRESH INDEX
# ============================================================================
print("\n=== SHOW INDEXES ===")
spark.sql(f"SHOW INDEXES FROM {table_name}").show(truncate=False)

print("\n=== REFRESH INDEX idx_driver ===")
try:
    spark.sql(f"REFRESH INDEX idx_driver ON {table_name}")
    print("REFRESH INDEX succeeded.")
except Exception as e:
    print(f"REFRESH INDEX FAILED: {root_cause(e)}")

# ============================================================================
# 4. Evidence the indexes are used
# ============================================================================
print("\n=== Metadata table partitions on disk ===")
metadata_dir = f"{table_path}/.hoodie/metadata"
for p in sorted(d for d in os.listdir(metadata_dir) if os.path.isdir(os.path.join(metadata_dir, d))):
    print(f"  {p}")

print("\n=== EXPLAIN for a query on the expression-indexed column ===")
spark.sql(f"""
  EXPLAIN SELECT id, driver, fare FROM {table_name} WHERE from_unixtime(ts, 'yyyy-MM-dd') = '1970-01-20'
""").show(truncate=False)

print("\n=== EXPLAIN for a query on the secondary-indexed column ===")
spark.sql(f"EXPLAIN SELECT id, rider, fare FROM {table_name} WHERE driver = '{drivers[0]}'").show(truncate=False)

print("\n=== Secondary-index lookup query (driver = a specific value) ===")
before = time.perf_counter()
hits = spark.sql(f"SELECT id, driver, city, fare FROM {table_name} WHERE driver = '{drivers[0]}'")
hit_count = hits.count()
elapsed_ms = int((time.perf_counter() - before) * 1000)
print(f"driver = '{drivers[0]}' matched {hit_count} rows in {elapsed_ms}ms")
hits.show(10, truncate=False)

print("\n=== Bloom-filter-indexed lookup (lower(rider) = a specific value) ===")
spark.sql(f"SELECT id, rider, fare FROM {table_name} WHERE lower(rider) = '{riders[0]}'").show(5, truncate=False)

# ============================================================================
# 5. DROP INDEX
# ============================================================================
print("\n=== DROP INDEX idx_bloom_rider ===")
spark.sql(f"DROP INDEX idx_bloom_rider ON {table_name}")
print("\n=== SHOW INDEXES after DROP ===")
spark.sql(f"SHOW INDEXES FROM {table_name}").show(truncate=False)
