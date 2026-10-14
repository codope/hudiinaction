// Chapter 9, "Storing semi-structured and unstructured data" - a support-ticket corpus
// showing VECTOR element types, BLOB storage modes, and hudi_vector_search().
//
// This is a NEW dataset (support tickets), not Chapter 2's product catalog: Chapter 2
// showed the first VECTOR/BLOB/hudi_vector_search taste with default-typed columns and
// an INLINE-only BLOB. This example goes one level deeper:
//   - two VECTOR element types on the same table (default FLOAT, and INT8)
//   - a BLOB column written INLINE and a separate BLOB column written OUT_OF_LINE
//     (an external file reference)
//   - hudi_vector_search() with all three distance metrics (cosine, l2, dot_product)
//   - hudi_vector_search_batch() against a second query table
//
// Everything here is plain Spark SQL so the DDL/DML surface is visible.

import java.io.File

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/ai_types")
val tablePath = s"$basePath/support_tickets"
val attachmentsDir = s"$basePath/attachments"
val tableName = "support_tickets"

// --- idempotent setup: wipe this example's own /tmp directory -------------------------
def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))
new File(attachmentsDir).mkdirs()
spark.sql(s"DROP TABLE IF EXISTS $tableName")

// External files that the OUT_OF_LINE BLOB column will reference. These stand in for
// call-center transcripts too large to want inlined into every base file.
val transcripts = Seq(
  "T-1001" -> "Customer reports the app crashes on login after the 4.2 update.",
  "T-1002" -> "Customer cannot reset their password; the reset email never arrives.",
  "T-1003" -> "Billing was charged twice for the same invoice this month.",
  "T-1004" -> "Customer asks how to export their data before closing the account.",
  "T-1005" -> "App crashes immediately after opening on Android 14 devices.",
  "T-1006" -> "Refund request for a duplicate charge flagged by the customer's bank."
)
val transcriptFiles = transcripts.map { case (id, text) =>
  val f = new File(attachmentsDir, s"$id.txt")
  val bytes = text.getBytes("UTF-8")
  val fos = new java.io.FileOutputStream(f)
  fos.write(bytes)
  fos.close()
  id -> (f.getCanonicalPath, bytes.length)
}.toMap

println(s"Wrote ${transcriptFiles.size} external transcript files under $attachmentsDir")

// ============================================================================
// 1. CREATE TABLE: two VECTOR element types + two BLOB storage modes
// ============================================================================
// - embedding      VECTOR(8)          -> default element type, FLOAT
// - embedding_i8   VECTOR(8, INT8)    -> quantized embedding, one byte per dimension
// - thumbnail      BLOB               -> written INLINE (small bytes in the row itself)
// - transcript     BLOB               -> written OUT_OF_LINE (reference to an external file)
spark.sql(s"""
  CREATE TABLE $tableName (
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
  LOCATION '$tablePath'
""")
println(s"Created table $tableName at $tablePath")
println("Schema:")
spark.table(tableName).printSchema()

// ============================================================================
// 2. INSERT: BLOB INLINE / OUT_OF_LINE named_struct literals
// ============================================================================
// The reference struct shape is fixed: struct<external_path:string, offset:bigint,
// length:bigint, managed:boolean>. An INLINE blob carries bytes in `data` and a null
// `reference`; an OUT_OF_LINE blob carries a null `data` and a populated `reference`.
val referenceStructType = "struct<external_path:string,offset:bigint,length:bigint,managed:boolean>"

def inlineBlob(bytesHex: String): String =
  s"named_struct('type','INLINE','data',cast(unhex('$bytesHex') as binary),'reference',cast(null as $referenceStructType))"

def outOfLineBlob(path: String, length: Long): String =
  s"named_struct('type','OUT_OF_LINE','data',cast(null as binary)," +
    s"'reference',named_struct('external_path','$path','offset',cast(0 as bigint)," +
    s"'length',cast($length as bigint),'managed',false))"

// A deterministic, made-up 8-dim embedding per ticket (no real model involved) so the
// vector search results below are easy to reason about by hand.
val tickets = Seq(
  ("T-1001", "App crashes on login",        "bug",      1001L, Array(0.9f, 0.1f, 0.0f, 0.0f, 0.2f, 0.0f, 0.1f, 0.0f)),
  ("T-1002", "Password reset email missing","account",  1002L, Array(0.1f, 0.9f, 0.1f, 0.0f, 0.0f, 0.1f, 0.0f, 0.0f)),
  ("T-1003", "Double-charged on invoice",    "billing",  1003L, Array(0.0f, 0.1f, 0.9f, 0.1f, 0.0f, 0.0f, 0.0f, 0.1f)),
  ("T-1004", "How to export account data",   "account",  1004L, Array(0.1f, 0.8f, 0.2f, 0.0f, 0.1f, 0.0f, 0.0f, 0.0f)),
  ("T-1005", "App crashes on Android 14",    "bug",      1005L, Array(0.85f,0.15f,0.0f, 0.05f,0.15f,0.0f, 0.1f, 0.0f)),
  ("T-1006", "Refund for duplicate charge",  "billing",  1006L, Array(0.0f, 0.05f,0.85f,0.15f,0.0f, 0.05f,0.0f, 0.1f))
)

val insertRows = tickets.map { case (id, subject, category, ts, emb) =>
  val (path, length) = transcriptFiles(id)
  val embLit = "array(" + emb.map(v => s"cast($v as float)").mkString(",") + ")"
  // INT8 embedding: scale the float embedding into the signed byte range so it is a
  // recognizably "quantized" version of the same vector, not unrelated noise.
  val embI8 = emb.map(v => math.round(v * 127f).toByte)
  val embI8Lit = "array(" + embI8.map(v => s"cast($v as tinyint)").mkString(",") + ")"
  val thumbHex = f"${math.abs(id.hashCode) % 256}%02x" * 8 // 8 bytes of placeholder thumbnail data
  s"('$id','$subject','$category',$ts,$embLit,$embI8Lit,${inlineBlob(thumbHex)},${outOfLineBlob(path, length)})"
}

spark.sql(s"INSERT INTO $tableName VALUES ${insertRows.mkString(",\n")}")
println(s"Inserted ${tickets.size} tickets")

// ============================================================================
// 3. read_blob() over both storage modes
// ============================================================================
println("\n=== read_blob() over INLINE (thumbnail) and OUT_OF_LINE (transcript) ===")
spark.sql(s"""
  SELECT ticket_id,
         length(read_blob(thumbnail))  AS thumbnail_bytes,
         length(read_blob(transcript)) AS transcript_bytes,
         string(read_blob(transcript)) AS transcript_text
  FROM $tableName
  ORDER BY ticket_id
""").show(false)

println("Raw BLOB struct shape (thumbnail = INLINE, transcript = OUT_OF_LINE):")
spark.sql(s"SELECT ticket_id, thumbnail, transcript FROM $tableName WHERE ticket_id = 'T-1001'")
  .show(false)

// ============================================================================
// 4. hudi_vector_search(): cosine, l2, dot_product
// ============================================================================
// Query vector close to the "bug"-category cluster.
val queryVec = "ARRAY(" + Array(0.88f, 0.12f, 0.0f, 0.0f, 0.18f, 0.0f, 0.1f, 0.0f).mkString(",") + ")"

for (metric <- Seq("cosine", "l2", "dot_product")) {
  println(s"\n=== hudi_vector_search() metric=$metric (top 3, ordered by _hudi_distance asc) ===")
  val df = spark.sql(s"""
    SELECT ticket_id, subject, category, _hudi_distance
    FROM hudi_vector_search('$tablePath', 'embedding', $queryVec, 3, '$metric')
    ORDER BY _hudi_distance
  """)
  df.show(false)
  println("Result schema: " + df.schema.simpleString)
}

// ============================================================================
// 5. hudi_vector_search_batch(): a second query table, one query per row
// ============================================================================
spark.sql("""
  CREATE OR REPLACE TEMP VIEW ticket_queries AS
  SELECT * FROM VALUES
    ('bug-like',     array(cast(0.87 as float), cast(0.13 as float), cast(0.0 as float), cast(0.0 as float), cast(0.2 as float), cast(0.0 as float), cast(0.1 as float), cast(0.0 as float))),
    ('billing-like', array(cast(0.0 as float),  cast(0.08 as float), cast(0.87 as float), cast(0.12 as float), cast(0.0 as float), cast(0.03 as float), cast(0.0 as float), cast(0.1 as float)))
  AS t(query_label, query_embedding)
""")

println("\n=== hudi_vector_search_batch() top 2 per query row ===")
val batchDf = spark.sql(s"""
  SELECT *
  FROM hudi_vector_search_batch('$tablePath', 'embedding', 'ticket_queries', 'query_embedding', 2, 'cosine')
  ORDER BY _hudi_query_index, _hudi_distance
""")
batchDf.show(false)
println("Batch result schema: " + batchDf.schema.simpleString)

System.exit(0)
