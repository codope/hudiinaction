// Chapter 9, "Storing binary payloads with BLOB" - the chapter's exact reduced snippet,
// run verbatim (only the external transcript path is a local /tmp path instead of s3://,
// per the run instructions; a real 63-byte transcript file is created on disk first).

import java.io.File

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/blob_support_tickets")
val tablePath = s"$basePath/support_tickets"
val transcriptPath = s"$basePath/transcripts/T-1001.txt"

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))
new File(s"$basePath/transcripts").mkdirs()

// Exactly 63 bytes, matching the chapter's `'length', 63L` literal.
val transcriptText = "Customer reports the app crashes on login after the 4.2 update."
val bytes = transcriptText.getBytes("UTF-8")
println(s"transcript byte length = ${bytes.length}")
val fos = new java.io.FileOutputStream(new File(transcriptPath))
fos.write(bytes)
fos.close()

spark.sql("DROP TABLE IF EXISTS support_tickets")

println("=== CHAPTER'S EXACT SNIPPET, VERBATIM (s3:// substituted with local /tmp path) ===")
spark.sql(s"""
  CREATE TABLE support_tickets (
    ticket_id  STRING,
    subject    STRING,
    created_at BIGINT,
    embedding  VECTOR(8),
    thumbnail  BLOB,
    transcript BLOB
  ) USING hudi
  TBLPROPERTIES (primaryKey = 'ticket_id', orderingFields = 'created_at', type = 'cow')
  LOCATION '$tablePath'
""")

spark.sql(s"""
  INSERT INTO support_tickets VALUES (
    'T-1001', 'App crashes on login', 1001,
    array(0.9, 0.1, 0.0, 0.0, 0.2, 0.0, 0.1, 0.0),
    named_struct('type', 'INLINE', 'data', unhex('C7C7C7C7C7C7C7C7'),
                 'reference', cast(null AS struct<external_path:string, offset:bigint,
                                                  length:bigint, managed:boolean>)),
    named_struct('type', 'OUT_OF_LINE', 'data', cast(null AS binary),
                 'reference', named_struct('external_path', '$transcriptPath',
                                           'offset', 0L, 'length', 63L, 'managed', false)))
""")

println("Insert succeeded.")

println("=== read_blob() SELECT, chapter's exact statement ===")
spark.sql("""
  SELECT ticket_id,
         length(read_blob(thumbnail))  AS thumbnail_bytes,
         string(read_blob(transcript)) AS transcript_text
  FROM support_tickets
""").show(false)

println("BLOB_SUPPORT_TICKETS_DONE")
System.exit(0)
