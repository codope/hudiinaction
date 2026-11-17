"""
Creates savepoints on the raw and silver tables, or restores both tables to them.
PySpark version of src/main/scala/.../governance/SavepointManager.scala.

  ./scripts/create_demo_savepoints_pyspark.sh
  ./scripts/restore_demo_savepoints_pyspark.sh
  # or: spark-submit <options> pyspark/savepoint_manager.py create|restore
"""

import os
import sys

from pyspark.sql import SparkSession

RAW_TABLE = "hudi_payments_raw"
SILVER_TABLE = "hudi_vendor_payouts"
RAW_PATH = "/tmp/hudi/raw/payments"
SILVER_PATH = "/tmp/hudi/silver/vendor_payouts"
SAVEPOINT_FILE = "/tmp/hudi/chapter06/savepoints.env"


def register_tables(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE}
        USING hudi
        LOCATION '{RAW_PATH}'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
        USING hudi
        LOCATION '{SILVER_PATH}'
    """)


def latest_commit(spark, table):
    commit_times = [r[0] for r in spark.sql(f"CALL show_commits(table => '{table}')").collect() if r[0] is not None]
    if not commit_times:
        raise RuntimeError(f"No commits found for table {table}")
    return max(commit_times)


def persist_savepoints(raw_commit, silver_commit):
    os.makedirs(os.path.dirname(SAVEPOINT_FILE), exist_ok=True)
    with open(SAVEPOINT_FILE, "w", encoding="utf-8") as f:
        f.write(f"RAW_SAVEPOINT={raw_commit}\nSILVER_SAVEPOINT={silver_commit}\n")


def load_savepoints():
    if not os.path.exists(SAVEPOINT_FILE):
        raise RuntimeError(f"Missing {SAVEPOINT_FILE}. Run create mode first.")
    with open(SAVEPOINT_FILE, encoding="utf-8") as f:
        values = dict(line.strip().split("=", 1) for line in f if "=" in line)
    for key in ("RAW_SAVEPOINT", "SILVER_SAVEPOINT"):
        if key not in values:
            raise RuntimeError(f"{key} not found")
    return values["RAW_SAVEPOINT"], values["SILVER_SAVEPOINT"]


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "create"

    spark = SparkSession.builder.appName("Chapter06SavepointManager").getOrCreate()

    register_tables(spark)

    if mode == "create":
        raw_commit = latest_commit(spark, RAW_TABLE)
        silver_commit = latest_commit(spark, SILVER_TABLE)

        spark.sql(f"CALL create_savepoint(table => '{RAW_TABLE}', commit_time => '{raw_commit}')")
        spark.sql(f"CALL create_savepoint(table => '{SILVER_TABLE}', commit_time => '{silver_commit}')")

        persist_savepoints(raw_commit, silver_commit)

        print(f"Created raw savepoint   : {raw_commit}")
        print(f"Created silver savepoint: {silver_commit}")
        print(f"Saved metadata to       : {SAVEPOINT_FILE}")

    elif mode == "restore":
        raw_savepoint, silver_savepoint = load_savepoints()

        spark.sql(f"CALL rollback_to_savepoint(table => '{RAW_TABLE}', instant_time => '{raw_savepoint}')")
        spark.sql(f"CALL rollback_to_savepoint(table => '{SILVER_TABLE}', instant_time => '{silver_savepoint}')")

        print(f"Restored raw    to {raw_savepoint}")
        print(f"Restored silver to {silver_savepoint}")

    else:
        raise ValueError(f"Unsupported mode '{mode}'. Use 'create' or 'restore'.")


if __name__ == "__main__":
    main()
