# Chapter 9, "Controlling concurrency" - early conflict detection under optimistic
# concurrency control (OCC), contrasted with plain OCC (conflict caught only at commit).
# PySpark version of spark/early_conflict.scala.
#
# Two writers in the SAME Spark application, on two Python threads, coordinated with
# InProcessLockProvider (safe for two writers inside one process on any filesystem; it is NOT a
# substitute for a distributed lock across separate processes or hosts).
#
# Both writers upsert the SAME record keys, which live in the SAME single file (the initial
# load is coalesced to one file), so both target the same file group and a conflict is
# certain, not probabilistic. Writer A is slowed down with a per-row sleep UDF. Writer B does
# NOT start on a fixed timer: it polls the table's own timeline directory and only begins
# once Writer A's instant is visibly ".inflight", so the "B starts before A commits" ordering
# is enforced by the filesystem, not by guessing a sleep duration.
#
# hoodie.write.markers.type is set explicitly. Spark's default, TIMELINE_SERVER_BASED, checks
# for conflicts asynchronously every hoodie.write.concurrency.async.conflict.detector.period_ms
# (30 s by default), far longer than this example waits. DIRECT markers check synchronously.

import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/early_conflict")
shutil.rmtree(base_path, ignore_errors=True)


@udf("string")
def slow_identity(s, ms):
    time.sleep(ms / 1000.0)
    return s


def seed_table(table_path):
    rows = (spark.createDataFrame([(f"id-{i}", f"driver-{i}", 10.0 + i, 1000 + i) for i in range(1, 11)],
                                  "id string, driver string, fare double, ts bigint")
            .coalesce(1))  # force exactly one file group so both writers are guaranteed to collide
    (rows.write.format("hudi")
        .option("hoodie.datasource.write.operation", "bulk_insert")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.table.ordering.fields", "ts")
        .option("hoodie.table.name", os.path.basename(table_path))
        .option("hoodie.bulkinsert.shuffle.parallelism", "1")
        .mode("overwrite")
        .save(table_path))


def common_write_options(table_path, early_detection, marker_type):
    return {
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.table.ordering.fields": "ts",
        "hoodie.table.name": os.path.basename(table_path),
        "hoodie.upsert.shuffle.parallelism": "1",
        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
        "hoodie.clean.failed.writes.policy": "LAZY",
        "hoodie.write.markers.type": marker_type,
        "hoodie.write.concurrency.early.conflict.detection.enable": str(early_detection).lower(),
    }


def timeline_entries(table_path):
    d = f"{table_path}/.hoodie/timeline"
    return sorted(f for f in os.listdir(d) if not f.endswith(".crc")) if os.path.exists(d) else []


# Instants that are still in flight. A completed instant keeps its .requested and .inflight
# files next to the completed <requested>_<completed>.<action> file, so an .inflight file
# alone does not mean the write is still running.
def pending_instants(table_path):
    entries = timeline_entries(table_path)
    completed = {f.split("_")[0] for f in entries if "_" in f}
    return [f.split(".")[0] for f in entries if f.endswith(".inflight") and f.split(".")[0] not in completed]


# Class and message of the innermost JVM exception behind a PySpark error.
def root_cause(e):
    j = getattr(e, "java_exception", None) or getattr(e, "_origin", None)
    if j is None:
        return f"{type(e).__name__}: {e}"
    while j.getCause() is not None and not j.getCause().equals(j):
        j = j.getCause()
    return f"{j.getClass().getName()}: {j.getMessage()}"


def run_scenario(label, early_detection, marker_type):
    tag = f"{'early' if early_detection else 'plain'}_{marker_type.lower()}"
    table_path = f"{base_path}/{tag}"
    seed_table(table_path)
    print("\n############################################################")
    print(f"# Scenario: {label} (early.detection={str(early_detection).lower()}, markers={marker_type})")
    print("############################################################")
    print(f"Timeline right after seed: {', '.join(timeline_entries(table_path))}")

    opts = common_write_options(table_path, early_detection, marker_type)

    def writer_a():
        t0 = time.perf_counter()
        try:
            df = (spark.createDataFrame([(f"id-{i}", f"driver-A-{i}", 20.0 + i, 2000 + i) for i in range(1, 11)],
                                        "id string, driver string, fare double, ts bigint")
                  .withColumn("driver", slow_identity(col("driver"), lit(700)))
                  .coalesce(1))
            df.write.format("hudi").options(**opts).mode("append").save(table_path)
            return True, int((time.perf_counter() - t0) * 1000), ""
        except Exception as e:
            return False, int((time.perf_counter() - t0) * 1000), root_cause(e)

    def writer_b():
        # Poll the timeline directory instead of sleeping a fixed duration: only start once
        # Writer A's instant is visibly .inflight (i.e. A has begun writing but not committed).
        deadline = time.time() + 15
        saw_inflight = False
        while not saw_inflight and time.time() < deadline:
            saw_inflight = bool(pending_instants(table_path))
            if not saw_inflight:
                time.sleep(0.02)
        print(f"Writer B observed Writer A's instant inflight = {str(saw_inflight).lower()}; "
              f"timeline now: {', '.join(timeline_entries(table_path))}")
        t0 = time.perf_counter()
        try:
            df = (spark.createDataFrame([(f"id-{i}", f"driver-B-{i}", 30.0 + i, 3000 + i) for i in range(1, 11)],
                                        "id string, driver string, fare double, ts bigint")
                  .coalesce(1))
            df.write.format("hudi").options(**opts).mode("append").save(table_path)
            return True, int((time.perf_counter() - t0) * 1000), ""
        except Exception as e:
            return False, int((time.perf_counter() - t0) * 1000), root_cause(e)

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_a = pool.submit(writer_a)
        fut_b = pool.submit(writer_b)
        ok_a, ms_a, err_a = fut_a.result(timeout=60)
        ok_b, ms_b, err_b = fut_b.result(timeout=60)

    print(f"Writer A: success={str(ok_a).lower():<5} elapsed={ms_a}ms" + (f" error={err_a}" if not ok_a else ""))
    print(f"Writer B: success={str(ok_b).lower():<5} elapsed={ms_b}ms" + (f" error={err_b}" if not ok_b else ""))
    print(f"Final timeline: {', '.join(timeline_entries(table_path))}")
    print(f"Final row count: {spark.read.format('hudi').load(table_path).count()}")


# --- Scenario 1: plain OCC, DIRECT markers, no early conflict detection ------------------
run_scenario("OCC only, DIRECT markers", False, "DIRECT")

# --- Scenario 2: OCC + early conflict detection, DIRECT markers (synchronous check) ------
run_scenario("OCC + early conflict detection, DIRECT markers", True, "DIRECT")

# --- Scenario 3: OCC + early conflict detection, TIMELINE_SERVER_BASED markers (async) ---
run_scenario("OCC + early conflict detection, TIMELINE_SERVER_BASED markers", True, "TIMELINE_SERVER_BASED")
