// Chapter 9, "Storing semi-structured and unstructured data" - observation only: inspect a
// written COW base file's Parquet footer schema to see how Hudi 1.2.0 actually lays out a
// VARIANT column on disk, and note whether it looks shredded (typed sub-columns alongside
// value/metadata) or unshredded (just the two binary fields). The 1.2 release notes and
// sql_ddl.md both say shredding has no user-facing control at 1.2.0 ("Only unshredded VARIANT
// is exposed at the user-facing level... every Spark/Hudi schema conversion produces an
// unshredded VARIANT"); this reads the actual footer rather than trusting that claim.
//
// Run scripts/run_variant_events.sh first so the events_cow table already has base files.

import org.apache.hadoop.fs.{FileSystem, Path}

val basePath = sys.env.getOrElse("VARIANT_BASE", "/tmp/hudiinaction/chapter09")
val cowPath  = s"$basePath/variant_cow/events"

val hadoopConf = spark.sparkContext.hadoopConfiguration
val fs = FileSystem.get(new java.net.URI(cowPath), hadoopConf)

def listParquetFiles(p: String): Seq[String] = {
  val it = fs.listFiles(new Path(p), true)
  val buf = scala.collection.mutable.ArrayBuffer[String]()
  while (it.hasNext) {
    val st = it.next()
    val name = st.getPath.getName
    if (name.endsWith(".parquet")) buf += st.getPath.toString
  }
  buf.toSeq
}

val files = listParquetFiles(cowPath)
println(s"Found ${files.size} base parquet file(s) under $cowPath")

if (files.nonEmpty) {
  val target = files.head
  println(s"Inspecting footer of: $target")
  val footer = org.apache.parquet.hadoop.ParquetFileReader.readFooter(
    hadoopConf, new Path(target), org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER)
  val schema = footer.getFileMetaData.getSchema
  println("=== Parquet MessageType schema ===")
  println(schema.toString)

  println("=== payload column group's leaf columns (looking for shredded typed_value / value / metadata) ===")
  import scala.jdk.CollectionConverters._
  val payloadGroup = schema.getFields.asScala.find(_.getName == "payload")
  payloadGroup match {
    case Some(f) =>
      println(s"payload field: $f")
      if (f.isPrimitive) {
        println("payload is a single primitive column (not shredded: no per-field typed columns).")
      } else {
        val group = f.asGroupType()
        val leafNames = group.getFields.asScala.map(_.getName)
        println(s"payload sub-fields: ${leafNames.mkString(", ")}")
        if (leafNames.toSet == Set("value", "metadata")) {
          println("OBSERVATION: exactly {value, metadata} present -> unshredded VARIANT, matches the docs.")
        } else {
          println("OBSERVATION: additional sub-fields present beyond {value, metadata} -> looks shredded.")
        }
      }
    case None =>
      println("No top-level 'payload' field found in this footer (unexpected).")
  }
}

println("VARIANT_INSPECT_SCHEMA_DONE")
System.exit(0)
