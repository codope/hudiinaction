# Chapter 9, "Upgrading and downgrading the table format" - create a table at an older
# table version, show hoodie.table.version, then upgrade (auto-upgrade on write, and the
# upgrade_table procedure) and downgrade (the downgrade_table procedure, and by-writer
# downgrade), showing the version after each step. Valid table versions at 1.2.x: SIX(6),
# EIGHT(8), NINE(9); NINE is the default hoodie.write.table.version.
# PySpark version of spark/table_version.scala.
#
# The DataFrame writes go to raw paths, not catalog tables: a catalog table's persisted
# TBLPROPERTIES are merged back into every later SQL write to it, which would pin the writer's
# target version and hide auto-upgrade. Auto-upgrade (hoodie.write.auto.upgrade, default true)
# checks only whether an upgrade is needed, so a lower hoodie.write.table.version than the
# table's current version does not take the same automatic path.

import os
import shutil

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

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/table_version")
shutil.rmtree(base_path, ignore_errors=True)
spark.sql("DROP TABLE IF EXISTS ch9_tv_cli_upgrade")


def table_version(table_path):
    with open(f"{table_path}/.hoodie/hoodie.properties") as f:
        return next((line.strip() for line in f if line.startswith("hoodie.table.version=")),
                    "hoodie.table.version=<not found>")


def write_df(path, rows, extra_opts, mode):
    (spark.createDataFrame(rows, "id string, fare double").write.format("hudi")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.table.ordering.fields", "id")
        .option("hoodie.table.name", os.path.basename(path))
        .options(**extra_opts)
        .mode(mode)
        .save(path))


# Class and message of the JVM exception behind a PySpark error, then its causes.
def java_error_chain(e):
    j = getattr(e, "java_exception", None) or getattr(e, "_origin", None)
    if j is None:
        return [(type(e).__name__, str(e))]
    chain = []
    while j is not None:
        chain.append((j.getClass().getName(), j.getMessage()))
        j = j.getCause()
    return chain


# ============================================================================
# (a) DataFrame write with hoodie.write.table.version=6, to a NEW path.
# ============================================================================
path_a = f"{base_path}/a_v6"
print("=== (a) DataFrame write, hoodie.write.table.version=6, new path ===")
write_df(path_a, [("r1", 12.0)], {"hoodie.write.table.version": "6"}, "overwrite")
print(table_version(path_a))

# ============================================================================
# (b) Second write to the SAME path, NO hoodie.write.table.version option (targets 9,
#     the default).
# ============================================================================
print("\n=== (b) Second write to the SAME path, no version option (writer targets 9 by default) ===")
try:
    write_df(path_a, [("r2", 18.0)], {}, "append")
    print("Write SUCCEEDED.")
except Exception as e:
    print(f"Write FAILED: {root_cause(e)}")
print(table_version(path_a))

# ============================================================================
# (c) Repeat (a) on a FRESH path, then write with hoodie.write.auto.upgrade=false and no
#     version option: does the table stay at 6, and does the write still succeed?
# ============================================================================
path_c = f"{base_path}/c_v6_noautoupgrade"
print("\n=== (c) Fresh path at version 6, then a write with hoodie.write.auto.upgrade=false ===")
write_df(path_c, [("r1", 12.0)], {"hoodie.write.table.version": "6"}, "overwrite")
print("After initial write: " + table_version(path_c))
try:
    write_df(path_c, [("r2", 18.0)], {"hoodie.write.auto.upgrade": "false"}, "append")
    print("Write with hoodie.write.auto.upgrade=false SUCCEEDED.")
except Exception as e:
    print(f"Write with hoodie.write.auto.upgrade=false FAILED: {root_cause(e)}")
print("After that write: " + table_version(path_c))

# ============================================================================
# (d) On a v9 table (default), write with hoodie.write.table.version=8: does the table
#     get auto-downgraded to 8, or does the write fail? Exact message either way.
# ============================================================================
path_d = f"{base_path}/d_v9_then_write_v8"
print("\n=== (d) Fresh v9 (default) table, then a write with hoodie.write.table.version=8 ===")
write_df(path_d, [("r1", 12.0)], {}, "overwrite")
print("After initial write (default version): " + table_version(path_d))
try:
    write_df(path_d, [("r2", 18.0)], {"hoodie.write.table.version": "8"}, "append")
    print("Write with hoodie.write.table.version=8 SUCCEEDED.")
except Exception as e:
    chain = java_error_chain(e)
    print("Write with hoodie.write.table.version=8 FAILED.")
    print(f"Exception class:   {chain[0][0]}")
    print(f"Exception message: {chain[0][1]}")
    for cls, msg in chain[1:]:
        print(f"  caused by: {cls}: {msg}")
print("After that write: " + table_version(path_d))

# ============================================================================
# 2. A second table, upgraded and downgraded explicitly via CALL upgrade_table /
#    CALL downgrade_table, which take a catalog table name.
# ============================================================================
cli_path = f"{base_path}/cli_upgrade"
spark.sql(f"""
  CREATE TABLE ch9_tv_cli_upgrade (
      id   STRING,
      fare DOUBLE
  ) USING hudi
  TBLPROPERTIES (
      primaryKey = 'id',
      'hoodie.write.table.version' = '6'
  )
  LOCATION '{cli_path}'
""")
spark.sql("INSERT INTO ch9_tv_cli_upgrade VALUES ('r1', 12.0)")
print("\n=== Catalog table created with hoodie.write.table.version=6 ===")
print(table_version(cli_path))

print("\n=== CALL upgrade_table(table => 'ch9_tv_cli_upgrade', to_version => 'EIGHT') ===")
try:
    spark.sql("CALL upgrade_table(table => 'ch9_tv_cli_upgrade', to_version => 'EIGHT')").show(truncate=False)
except Exception as e:
    print(f"upgrade_table FAILED: {root_cause(e)}")
print(table_version(cli_path))

print("\n=== CALL downgrade_table(table => 'ch9_tv_cli_upgrade', to_version => 'SIX') ===")
try:
    spark.sql("CALL downgrade_table(table => 'ch9_tv_cli_upgrade', to_version => 'SIX')").show(truncate=False)
except Exception as e:
    print(f"downgrade_table FAILED: {root_cause(e)}")
print(table_version(cli_path))

print("\n=== CALL upgrade_table with a NUMERIC to_version ('8') instead of the enum name ===")
try:
    spark.sql("CALL upgrade_table(table => 'ch9_tv_cli_upgrade', to_version => '8')").show(truncate=False)
    print(table_version(cli_path))
except Exception as e:
    print(f"upgrade_table with numeric version FAILED: {root_cause(e)}")
