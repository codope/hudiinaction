# Chapter 9, "Storing semi-structured and unstructured data" - a support-ticket corpus
# showing VECTOR element types, BLOB storage modes, and hudi_vector_search().
# PySpark version of spark/ai_types.scala.
#
# This is a NEW dataset (support tickets), not Chapter 2's product catalog: Chapter 2
# showed the first VECTOR/BLOB/hudi_vector_search taste with default-typed columns and
# an INLINE-only BLOB. This example goes one level deeper:
#   - two VECTOR element types on the same table (default FLOAT, and INT8)
#   - a BLOB column written INLINE and a separate BLOB column written OUT_OF_LINE
#     (an external file reference)
#   - hudi_vector_search() with all three distance metrics (cosine, l2, dot_product)
#   - hudi_vector_search_batch() against a second query table
#
# Everything here is plain Spark SQL so the DDL/DML surface is visible.

import math
import os
import shutil

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/ai_types")
table_path = f"{base_path}/support_tickets"
attachments_dir = f"{base_path}/attachments"
table_name = "support_tickets"

# --- idempotent setup: wipe this example's own /tmp directory -------------------------
shutil.rmtree(base_path, ignore_errors=True)
os.makedirs(attachments_dir)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# External files that the OUT_OF_LINE BLOB column will reference. These stand in for
# call-center transcripts too large to want inlined into every base file.
transcripts = {
    "T-1001": "Customer reports the app crashes on login after the 4.2 update.",
    "T-1002": "Customer cannot reset their password; the reset email never arrives.",
    "T-1003": "Billing was charged twice for the same invoice this month.",
    "T-1004": "Customer asks how to export their data before closing the account.",
    "T-1005": "App crashes immediately after opening on Android 14 devices.",
    "T-1006": "Refund request for a duplicate charge flagged by the customer's bank.",
}
transcript_files = {}
for ticket_id, text in transcripts.items():
    path = os.path.join(attachments_dir, f"{ticket_id}.txt")
    data = text.encode("utf-8")
    with open(path, "wb") as f:
        f.write(data)
    transcript_files[ticket_id] = (os.path.realpath(path), len(data))

print(f"Wrote {len(transcript_files)} external transcript files under {attachments_dir}")

# ============================================================================
# 1. CREATE TABLE: two VECTOR element types + two BLOB storage modes
# ============================================================================
# - embedding      VECTOR(8)          -> default element type, FLOAT
# - embedding_i8   VECTOR(8, INT8)    -> quantized embedding, one byte per dimension
# - thumbnail      BLOB               -> written INLINE (small bytes in the row itself)
# - transcript     BLOB               -> written OUT_OF_LINE (reference to an external file)
spark.sql(f"""
  CREATE TABLE {table_name} (
      ticket_id    STRING,
      subject      STRING,
      category     STRING,
      created_at   BIGINT,
      embedding    VECTOR(8)        COMMENT 'subject embedding, default FLOAT element type',
      embedding_i8 VECTOR(8, INT8)  COMMENT 'quantized subject embedding, INT8 element type',
      thumbnail    BLOB             COMMENT 'small screenshot thumbnail, stored INLINE',
      transcript   BLOB             COMMENT 'full call transcript, stored OUT_OF_LINE'
  ) USING hudi
  TBLPROPERTIES (
      primaryKey = 'ticket_id',
      orderingFields = 'created_at',
      type = 'cow'
  )
  LOCATION '{table_path}'
""")
print(f"Created table {table_name} at {table_path}")
print("Schema:")
spark.table(table_name).printSchema()

# ============================================================================
# 2. INSERT: BLOB INLINE / OUT_OF_LINE named_struct literals
# ============================================================================
# The reference struct shape is fixed: struct<external_path:string, offset:bigint,
# length:bigint, managed:boolean>. An INLINE blob carries bytes in `data` and a null
# `reference`; an OUT_OF_LINE blob carries a null `data` and a populated `reference`.
reference_struct_type = "struct<external_path:string,offset:bigint,length:bigint,managed:boolean>"


def inline_blob(bytes_hex):
    return (f"named_struct('type','INLINE','data',cast(unhex('{bytes_hex}') as binary),"
            f"'reference',cast(null as {reference_struct_type}))")


def out_of_line_blob(path, length):
    return ("named_struct('type','OUT_OF_LINE','data',cast(null as binary),"
            f"'reference',named_struct('external_path','{path}','offset',cast(0 as bigint),"
            f"'length',cast({length} as bigint),'managed',false))")


# Java's String.hashCode, so the placeholder thumbnails match the Scala version byte for byte.
def java_string_hash(s):
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    return h - (1 << 32) if h >= (1 << 31) else h


# A deterministic, made-up 8-dim embedding per ticket (no real model involved) so the
# vector search results below are easy to reason about by hand.
tickets = [
    ("T-1001", "App crashes on login",         "bug",     1001, [0.9, 0.1, 0.0, 0.0, 0.2, 0.0, 0.1, 0.0]),
    ("T-1002", "Password reset email missing", "account", 1002, [0.1, 0.9, 0.1, 0.0, 0.0, 0.1, 0.0, 0.0]),
    ("T-1003", "Double-charged on invoice",    "billing", 1003, [0.0, 0.1, 0.9, 0.1, 0.0, 0.0, 0.0, 0.1]),
    ("T-1004", "How to export account data",   "account", 1004, [0.1, 0.8, 0.2, 0.0, 0.1, 0.0, 0.0, 0.0]),
    ("T-1005", "App crashes on Android 14",    "bug",     1005, [0.85, 0.15, 0.0, 0.05, 0.15, 0.0, 0.1, 0.0]),
    ("T-1006", "Refund for duplicate charge",  "billing", 1006, [0.0, 0.05, 0.85, 0.15, 0.0, 0.05, 0.0, 0.1]),
]

insert_rows = []
for ticket_id, subject, category, ts, emb in tickets:
    path, length = transcript_files[ticket_id]
    emb_lit = "array(" + ",".join(f"cast({v} as float)" for v in emb) + ")"
    # INT8 embedding: scale the float embedding into the signed byte range so it is a
    # recognizably "quantized" version of the same vector, not unrelated noise.
    emb_i8 = [int(math.floor(v * 127 + 0.5)) for v in emb]
    emb_i8_lit = "array(" + ",".join(f"cast({v} as tinyint)" for v in emb_i8) + ")"
    thumb_hex = f"{abs(java_string_hash(ticket_id)) % 256:02x}" * 8  # 8 bytes of placeholder thumbnail data
    insert_rows.append(f"('{ticket_id}','{subject}','{category}',{ts},{emb_lit},{emb_i8_lit},"
                       f"{inline_blob(thumb_hex)},{out_of_line_blob(path, length)})")

spark.sql(f"INSERT INTO {table_name} VALUES " + ",\n".join(insert_rows))
print(f"Inserted {len(tickets)} tickets")

# ============================================================================
# 3. read_blob() over both storage modes
# ============================================================================
print("\n=== read_blob() over INLINE (thumbnail) and OUT_OF_LINE (transcript) ===")
spark.sql(f"""
  SELECT ticket_id,
         length(read_blob(thumbnail))  AS thumbnail_bytes,
         length(read_blob(transcript)) AS transcript_bytes,
         string(read_blob(transcript)) AS transcript_text
  FROM {table_name}
  ORDER BY ticket_id
""").show(truncate=False)

print("Raw BLOB struct shape (thumbnail = INLINE, transcript = OUT_OF_LINE):")
spark.sql(f"SELECT ticket_id, thumbnail, transcript FROM {table_name} WHERE ticket_id = 'T-1001'").show(truncate=False)

# ============================================================================
# 4. hudi_vector_search(): cosine, l2, dot_product
# ============================================================================
# Query vector close to the "bug"-category cluster.
query_vec = "ARRAY(" + ",".join(str(v) for v in [0.88, 0.12, 0.0, 0.0, 0.18, 0.0, 0.1, 0.0]) + ")"

for metric in ["cosine", "l2", "dot_product"]:
    print(f"\n=== hudi_vector_search() metric={metric} (top 3, ordered by _hudi_distance asc) ===")
    df = spark.sql(f"""
      SELECT ticket_id, subject, category, _hudi_distance
      FROM hudi_vector_search('{table_path}', 'embedding', {query_vec}, 3, '{metric}')
      ORDER BY _hudi_distance
    """)
    df.show(truncate=False)
    print("Result schema: " + df.schema.simpleString())

# ============================================================================
# 5. hudi_vector_search_batch(): a second query table, one query per row
# ============================================================================
spark.sql("""
  CREATE OR REPLACE TEMP VIEW ticket_queries AS
  SELECT * FROM VALUES
    ('bug-like',     array(cast(0.87 as float), cast(0.13 as float), cast(0.0 as float), cast(0.0 as float), cast(0.2 as float), cast(0.0 as float), cast(0.1 as float), cast(0.0 as float))),
    ('billing-like', array(cast(0.0 as float),  cast(0.08 as float), cast(0.87 as float), cast(0.12 as float), cast(0.0 as float), cast(0.03 as float), cast(0.0 as float), cast(0.1 as float)))
  AS t(query_label, query_embedding)
""")

print("\n=== hudi_vector_search_batch() top 2 per query row ===")
batch_df = spark.sql(f"""
  SELECT *
  FROM hudi_vector_search_batch('{table_path}', 'embedding', 'ticket_queries', 'query_embedding', 2, 'cosine')
  ORDER BY _hudi_query_index, _hudi_distance
""")
batch_df.show(truncate=False)
print("Batch result schema: " + batch_df.schema.simpleString())
