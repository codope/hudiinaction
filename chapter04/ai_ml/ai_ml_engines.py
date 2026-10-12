"""
Chapter 4: reading the store_sales Hudi table from Python engines without Spark.

  python ai_ml_engines.py [hudi] [ray] [daft]

Prerequisites
- /tmp/hudi_demo_tbl, created by hudi_querying_tutorial.scala and updated by query_types.scala
  (Hudi 1.2.1, table version 9).
- /tmp/hudi_demo_tbl_v6, a table-version-6 copy for Daft, created by make_v6_copy.scala.
- pip install -r requirements.txt

Pass the commit times printed by CALL show_commits(table => 'hudi_tbl') through the
environment:
  LOAD_COMMIT_TIME=<commit_time of the bulk_insert>
  LOAD_COMPLETION_TIME=<state_transition_time of the bulk_insert>
"""
import os
import sys

TABLE = "/tmp/hudi_demo_tbl"
TABLE_V6 = "/tmp/hudi_demo_tbl_v6"
LOAD_COMMIT_TIME = os.environ.get("LOAD_COMMIT_TIME")
LOAD_COMPLETION_TIME = os.environ.get("LOAD_COMPLETION_TIME")

which = sys.argv[1:] or ["hudi", "ray", "daft"]


def section(name):
    print(f"\n######## {name}", flush=True)


if "hudi" in which:
    section("hudi-rs (Python bindings)")
    import pyarrow as pa
    from hudi import HudiReadOptions, HudiTableBuilder

    hudi_table = HudiTableBuilder.from_base_uri(TABLE).build()

    # Snapshot read of the latest committed state.
    snapshot = pa.Table.from_batches(hudi_table.read(HudiReadOptions()))
    print("snapshot rows:", snapshot.num_rows)

    # Time travel: the table as of the initial load.
    if LOAD_COMMIT_TIME:
        opts = HudiReadOptions().with_as_of_timestamp(LOAD_COMMIT_TIME)
        as_of = pa.Table.from_batches(hudi_table.read(opts))
        print("as-of rows:", as_of.num_rows)

if "ray" in which:
    section("Ray Data")
    import ray

    ray.init(ignore_reinit_error=True)

    # 1) Snapshot read and vectorized feature engineering.
    ds = ray.data.read_hudi(TABLE)

    def prep(batch):
        list_price = batch["ss_ext_list_price"].astype(float)
        discount = batch["ss_ext_discount_amt"].astype(float)
        batch["discount_ratio"] = (discount / (list_price + 1)).clip(0, 1)
        return batch

    prepped = ds.map_batches(prep, batch_format="pandas")
    print("snapshot rows:", prepped.count())
    print(prepped.select_columns(["ss_item_sk", "ss_ticket_number", "discount_ratio"]).take(2))

    # 2) Incremental read: rows changed after the initial load.
    for label, start in (("completion", LOAD_COMPLETION_TIME), ("commit", LOAD_COMMIT_TIME)):
        if not start:
            continue
        incr = ray.data.read_hudi(
            TABLE,
            query_type="incremental",
            hudi_options={"hoodie.read.file_group.start_timestamp": start},
        )
        print(f"incremental rows (start = load {label} time {start}):", incr.count())

if "daft" in which:
    section("Daft")
    import daft

    for path in (TABLE_V6, TABLE):
        try:
            df = daft.read_hudi(path)
            features = (
                df.where(df["ss_store_sk"] == 7)
                .groupby("ss_customer_sk")
                .agg(
                    daft.col("ss_quantity").sum().alias("total_qty"),
                    daft.col("ss_ticket_number").count().alias("tickets"),
                )
            )
            print(path, "store-7 customers:", features.count_rows())
            features.show(3)
        except Exception as e:  # report and continue, the v9 table is expected to fail
            print(path, "FAILED:", type(e).__name__, str(e)[:300])
