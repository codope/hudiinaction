"""
Chapter 4: reading the store_sales Hudi table from Python with hudi-rs, without Spark.

  pip install -r requirements.txt
  python ai_ml_engines.py <commit_time>

Prerequisite: /tmp/hudi_demo_tbl, created by hudi_querying_tutorial.scala (Hudi 1.2.1, table
version 9). hudi-rs 0.5.0 is the first release that reads table versions 8 and 9.
Pass a commit_time from CALL show_commits(table => 'hudi_tbl') for the time-travel read.
"""
import sys

import pyarrow as pa
from hudi import HudiReadOptions, HudiTableBuilder

hudi_table = HudiTableBuilder.from_base_uri("/tmp/hudi_demo_tbl").build()

# Snapshot read of the latest committed state.
snapshot = pa.Table.from_batches(hudi_table.read(HudiReadOptions()))
print("snapshot rows:", snapshot.num_rows)          # 2880404

# Time travel: the table as of a commit
if len(sys.argv) > 1:
    options = HudiReadOptions().with_as_of_timestamp(sys.argv[1])
    arrow_tbl = pa.Table.from_batches(hudi_table.read(options))
    print(arrow_tbl.schema, arrow_tbl.num_rows)     # 2880404 rows as of the load commit
