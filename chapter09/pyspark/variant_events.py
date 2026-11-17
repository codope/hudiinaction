# Chapter 9, "Storing semi-structured and unstructured data" - VARIANT on Spark 4.x.
# PySpark version of spark/variant_events.scala.
#
# Creates two Hudi tables (COW and MOR) with a native VARIANT column holding semi-structured
# event payloads, inserts rows via parse_json(), upserts one row, and queries the payload with
# variant_get() and the `:` path shorthand. Run with `scripts/run_pyspark.sh variant_events`,
# which uses Spark 4.1 (and needs Python 3.10 or later for PySpark 4.1).

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

base_path = os.environ.get("VARIANT_BASE", "/tmp/hudiinaction/chapter09")
cow_path = f"{base_path}/variant_cow/agent_events"
mor_path = f"{base_path}/variant_mor/agent_events_mor"

spark.sql("DROP TABLE IF EXISTS agent_events")
spark.sql("DROP TABLE IF EXISTS agent_events_mor")


def create_events_table(table_name, path, table_type):
    spark.sql(f"""
      CREATE TABLE {table_name} (
          event_id  STRING,
          payload   VARIANT,
          ts        BIGINT
      ) USING hudi
      TBLPROPERTIES (
          primaryKey = 'event_id',
          orderingFields = 'ts',
          type = '{table_type}'
      )
      LOCATION '{path}'
    """)


create_events_table("agent_events", cow_path, "cow")
create_events_table("agent_events_mor", mor_path, "mor")


# parse_json() is a Spark 4.0+ built-in that returns a VariantType value.
def insert_rows(table_name):
    spark.sql(f"""
      INSERT INTO {table_name} VALUES
        ('e1', parse_json('{{"tool_calls":[{{"name":"search","args":{{"q":"hudi variant"}}}}],"latency_ms":42}}'), 1000),
        ('e2', parse_json('{{"tool_calls":[{{"name":"fetch","args":{{"url":"https://example.com"}}}}],"latency_ms":17}}'), 1001),
        ('e3', parse_json('{{"tool_calls":[],"latency_ms":3,"note":"no-op"}}'), 1002)
    """)


insert_rows("agent_events")
insert_rows("agent_events_mor")

print("=== CHAPTER'S EXACT SNIPPET, VERBATIM, against agent_events (COW) ===")
spark.sql("""
  SELECT event_id,
         variant_get(payload, '$.latency_ms', 'int')           AS latency_ms,
         variant_get(payload, '$.tool_calls[0].name', 'string') AS first_tool
  FROM agent_events
""").show()

for table in ["agent_events", "agent_events_mor"]:
    print(f"=== {table} after insert: variant_get + `:` path syntax (extra, not in chapter) ===")
    spark.sql(f"""
      SELECT event_id,
             variant_get(payload, '$.latency_ms', 'int')      AS latency_ms,
             payload:tool_calls[0].name::string                AS first_tool,
             CAST(payload AS STRING)                           AS payload_json
      FROM {table} ORDER BY event_id
    """).show(truncate=False)


# Upsert: same key e2, new payload and a null payload for e3 to exercise the "VARIANT set to
# NULL by an update" shape called out in the 1.2.1 known-issues note.
def upsert_rows(table_name):
    spark.sql(f"""
      MERGE INTO {table_name} t
      USING (
        SELECT 'e2' AS event_id, parse_json('{{"tool_calls":[{{"name":"fetch","args":{{"url":"https://example.com/v2"}}}}],"latency_ms":9}}') AS payload, 2001L AS ts
        UNION ALL
        SELECT 'e3' AS event_id, CAST(NULL AS VARIANT) AS payload, 2002L AS ts
      ) s
      ON t.event_id = s.event_id
      WHEN MATCHED THEN UPDATE SET t.payload = s.payload, t.ts = s.ts
    """)


upsert_rows("agent_events")
upsert_rows("agent_events_mor")

print("=== agent_events after upsert (e2 changed, e3 payload set to NULL) ===")
spark.sql("""
  SELECT event_id, payload IS NULL AS payload_is_null,
         variant_get(payload, '$.latency_ms', 'int') AS latency_ms
  FROM agent_events ORDER BY event_id
""").show(truncate=False)

print("=== agent_events_mor after upsert, default snapshot read (read-optimized would skip logs) ===")
spark.sql("""
  SELECT event_id, payload IS NULL AS payload_is_null,
         variant_get(payload, '$.latency_ms', 'int') AS latency_ms
  FROM agent_events_mor ORDER BY event_id
""").show(truncate=False)

print("VARIANT_EVENTS_DONE")
