"""
Chapter 4: Ray Data reading a Hudi Copy-on-Write table (snapshot and incremental).

Tested with Ray 2.59.0 and hudi-rs 0.4.0 (pip install -r requirements-ray.txt). Ray's read_hudi
is an alpha API. Create the table first with make_ray_table.scala, then:
  python ray_data_example.py <first commit time> <second commit time>
"""
import sys

import ray
import ray.data as rd

start_ts, end_ts = sys.argv[1], sys.argv[2]
ray.init()  # or ray.init(address="auto") on a cluster

# 1) Snapshot read with a partition filter
ds = rd.read_hudi(
    table_uri="/tmp/ss_cow_v6_part",
    query_type="snapshot",
    filters=[("ss_store_sk", "=", "7")],  # applies to partition columns
)


# Vectorized preprocessing -> to Torch/TF/XGBoost
def prep(batch):
    list_price = batch["ss_ext_list_price"].astype(float)
    discount = batch["ss_ext_discount_amt"].astype(float)
    batch["discount_ratio"] = (discount / (list_price + 1)).clip(0, 1)
    return batch


prepped = ds.map_batches(prep, batch_format="pandas")
print("store 7 rows:", prepped.count())          # 5113 in our run

# 2) Incremental read: records changed after start_ts, up to end_ts
incr = rd.read_hudi(
    table_uri="/tmp/ss_cow_v6_part",
    query_type="incremental",
    hudi_options={
        "hoodie.read.file_group.start_timestamp": start_ts,
        "hoodie.read.file_group.end_timestamp": end_ts,
    },
)  # Feed just the delta to an inference or retraining job
print("incremental rows:", incr.count())         # 20, the upserted rows
