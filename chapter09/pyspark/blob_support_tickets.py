# Chapter 9, "Storing binary payloads with BLOB" - the chapter's exact reduced snippet,
# run verbatim (only the external transcript path is a local /tmp path instead of s3://,
# per the run instructions; a real 63-byte transcript file is created on disk first).
# PySpark version of spark/blob_support_tickets.scala.

import os
import shutil

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("BASE_PATH", "/tmp/hudiinaction/chapter09/blob_support_tickets")
table_path = f"{base_path}/support_tickets"
transcript_path = f"{base_path}/transcripts/T-1001.txt"

shutil.rmtree(base_path, ignore_errors=True)
os.makedirs(f"{base_path}/transcripts")

# Exactly 63 bytes, matching the chapter's `'length', 63L` literal.
transcript_text = "Customer reports the app crashes on login after the 4.2 update."
data = transcript_text.encode("utf-8")
print(f"transcript byte length = {len(data)}")
with open(transcript_path, "wb") as f:
    f.write(data)

spark.sql("DROP TABLE IF EXISTS support_tickets")

print("=== CHAPTER'S EXACT SNIPPET, VERBATIM (s3:// substituted with local /tmp path) ===")
spark.sql(f"""
  CREATE TABLE support_tickets (
    ticket_id  STRING,
    subject    STRING,
    created_at BIGINT,
    embedding  VECTOR(8),
    thumbnail  BLOB,
    transcript BLOB
  ) USING hudi
  TBLPROPERTIES (primaryKey = 'ticket_id', orderingFields = 'created_at', type = 'cow')
  LOCATION '{table_path}'
""")

spark.sql(f"""
  INSERT INTO support_tickets VALUES (
    'T-1001', 'App crashes on login', 1001,
    array(0.9, 0.1, 0.0, 0.0, 0.2, 0.0, 0.1, 0.0),
    named_struct('type', 'INLINE', 'data', unhex('C7C7C7C7C7C7C7C7'),
                 'reference', cast(null AS struct<external_path:string, offset:bigint,
                                                  length:bigint, managed:boolean>)),
    named_struct('type', 'OUT_OF_LINE', 'data', cast(null AS binary),
                 'reference', named_struct('external_path', '{transcript_path}',
                                           'offset', 0L, 'length', 63L, 'managed', false)))
""")

print("Insert succeeded.")

print("=== read_blob() SELECT, chapter's exact statement ===")
spark.sql("""
  SELECT ticket_id,
         length(read_blob(thumbnail))  AS thumbnail_bytes,
         string(read_blob(transcript)) AS transcript_text
  FROM support_tickets
""").show(truncate=False)

print("BLOB_SUPPORT_TICKETS_DONE")
