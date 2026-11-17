# Chapter 9, "Storing semi-structured and unstructured data" - observation only: inspect a
# written COW base file's Parquet footer schema to see how Hudi 1.2.x actually lays out a
# VARIANT column on disk, and note whether it looks shredded (typed sub-columns alongside
# value/metadata) or unshredded (just the two binary fields). Python version of
# spark/variant_inspect_parquet_schema.scala.
#
# This one needs no Spark: it reads the footer with pyarrow (pip install pyarrow).
# Run variant_events.py first so the agent_events table already has base files.

import os

import pyarrow.parquet as pq

base_path = os.environ.get("VARIANT_BASE", "/tmp/hudiinaction/chapter09")
cow_path = f"{base_path}/variant_cow/agent_events"

files = sorted(os.path.join(d, f) for d, _, names in os.walk(cow_path)
               if "/.hoodie" not in d for f in names if f.endswith(".parquet"))
print(f"Found {len(files)} base parquet file(s) under {cow_path}")

if files:
    target = files[0]
    print(f"Inspecting footer of: {target}")
    schema = pq.ParquetFile(target).schema
    print("=== Parquet schema ===")
    print(str(schema).split("\n", 1)[1])   # drop the first line, an object repr

    print("=== payload column group's leaf columns (looking for shredded typed_value / value / metadata) ===")
    payload_leaves = [schema.column(i).path for i in range(len(schema)) if schema.column(i).path.startswith("payload.")]
    if not payload_leaves:
        print("No top-level 'payload' group found in this footer (unexpected).")
    else:
        sub_fields = sorted({p.split(".")[1] for p in payload_leaves})
        print(f"payload sub-fields: {', '.join(sub_fields)}")
        if set(sub_fields) == {"value", "metadata"}:
            print("OBSERVATION: exactly {value, metadata} present -> unshredded VARIANT, matches the docs.")
        else:
            print("OBSERVATION: additional sub-fields present beyond {value, metadata} -> looks shredded.")

print("VARIANT_INSPECT_SCHEMA_DONE")
