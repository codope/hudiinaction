// Chapter 9, "Storing semi-structured and unstructured data" - VARIANT on Spark 4.x.
//
// Creates two Hudi tables (COW and MOR) with a native VARIANT column holding semi-structured
// event payloads, inserts rows via parse_json(), upserts one row, and queries the payload with
// variant_get() and the `:` path shorthand. Run with scripts/spark4_shell_with_hudi_variant.sh.
//
// v2: uses orderingFields (not preCombineField) and the table name agent_events for the COW
// table (chapter's exact name); the MOR table used for the pushdown-issue demo is renamed
// agent_events_mor for consistency.

val basePath = sys.env.getOrElse("VARIANT_BASE", "/tmp/hudiinaction/chapter09")
val cowPath  = s"$basePath/variant_cow/agent_events"
val morPath  = s"$basePath/variant_mor/agent_events_mor"

spark.sql("DROP TABLE IF EXISTS agent_events")
spark.sql("DROP TABLE IF EXISTS agent_events_mor")

def createEventsTable(tableName: String, path: String, tableType: String): Unit = {
  spark.sql(s"""
    CREATE TABLE $tableName (
        event_id  STRING,
        payload   VARIANT,
        ts        BIGINT
    ) USING hudi
    TBLPROPERTIES (
        primaryKey = 'event_id',
        orderingFields = 'ts',
        type = '$tableType'
    )
    LOCATION '$path'
  """)
}

createEventsTable("agent_events", cowPath, "cow")
createEventsTable("agent_events_mor", morPath, "mor")

// parse_json() is a Spark 4.0+ built-in that returns a VariantType value.
def insertRows(tableName: String): Unit = {
  spark.sql(s"""
    INSERT INTO $tableName VALUES
      ('e1', parse_json('{"tool_calls":[{"name":"search","args":{"q":"hudi variant"}}],"latency_ms":42}'), 1000),
      ('e2', parse_json('{"tool_calls":[{"name":"fetch","args":{"url":"https://example.com"}}],"latency_ms":17}'), 1001),
      ('e3', parse_json('{"tool_calls":[],"latency_ms":3,"note":"no-op"}'), 1002)
  """)
}

insertRows("agent_events")
insertRows("agent_events_mor")

println("=== CHAPTER'S EXACT SNIPPET, VERBATIM, against agent_events (COW) ===")
spark.sql("""
  SELECT event_id,
         variant_get(payload, '$.latency_ms', 'int')           AS latency_ms,
         variant_get(payload, '$.tool_calls[0].name', 'string') AS first_tool
  FROM agent_events
""").show()

println("=== agent_events after insert: variant_get + `:` path syntax (extra, not in chapter) ===")
spark.sql("""
  SELECT event_id,
         variant_get(payload, '$.latency_ms', 'int')      AS latency_ms,
         payload:tool_calls[0].name::string                AS first_tool,
         CAST(payload AS STRING)                           AS payload_json
  FROM agent_events ORDER BY event_id
""").show(false)

println("=== agent_events_mor after insert: variant_get + `:` path syntax (extra, not in chapter) ===")
spark.sql("""
  SELECT event_id,
         variant_get(payload, '$.latency_ms', 'int')      AS latency_ms,
         payload:tool_calls[0].name::string                AS first_tool,
         CAST(payload AS STRING)                           AS payload_json
  FROM agent_events_mor ORDER BY event_id
""").show(false)

// Upsert: same key e2, new payload and a null payload for e3 to exercise the "VARIANT set to
// NULL by an update" shape called out in the 1.2.1 known-issues note.
def upsertRows(tableName: String): Unit = {
  spark.sql(s"""
    MERGE INTO $tableName t
    USING (
      SELECT 'e2' AS event_id, parse_json('{"tool_calls":[{"name":"fetch","args":{"url":"https://example.com/v2"}}],"latency_ms":9}') AS payload, 2001L AS ts
      UNION ALL
      SELECT 'e3' AS event_id, CAST(NULL AS VARIANT) AS payload, 2002L AS ts
    ) s
    ON t.event_id = s.event_id
    WHEN MATCHED THEN UPDATE SET t.payload = s.payload, t.ts = s.ts
  """)
}

upsertRows("agent_events")
upsertRows("agent_events_mor")

println("=== agent_events after upsert (e2 changed, e3 payload set to NULL) ===")
spark.sql("""
  SELECT event_id, payload IS NULL AS payload_is_null,
         variant_get(payload, '$.latency_ms', 'int') AS latency_ms
  FROM agent_events ORDER BY event_id
""").show(false)

println("=== agent_events_mor after upsert, default snapshot read (read-optimized would skip logs) ===")
spark.sql("""
  SELECT event_id, payload IS NULL AS payload_is_null,
         variant_get(payload, '$.latency_ms', 'int') AS latency_ms
  FROM agent_events_mor ORDER BY event_id
""").show(false)

println("VARIANT_EVENTS_DONE")
System.exit(0)
