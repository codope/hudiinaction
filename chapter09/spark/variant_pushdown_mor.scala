// Chapter 9, "Storing semi-structured and unstructured data" - reproduces the documented
// Spark 4.1 + MOR VARIANT read issue from the Hudi 1.2 release notes' Known Issues section
// (spark.sql.variant.pushVariantIntoScan, on by default on Spark 4.1, off by default on
// Spark 4.0): "A top-level VARIANT value set to null by an update is read back as a struct
// of nulls instead of NULL, so a filter such as `v IS NULL` misses the row." A fix is
// tracked in https://github.com/apache/hudi/pull/20039; the corroborating GitHub issue is
// https://github.com/apache/hudi/issues/20037.
//
// Run scripts/run_variant_events.sh first: this reads the agent_events_mor table it leaves
// behind, where the upsert set e3's payload to NULL and the update landed in an
// uncompacted .log file (MOR), which is the shape the known issue requires ("log files
// are merged"). Requires Spark 4.1.x specifically: the flag defaults to false on Spark 4.0.

val basePath = sys.env.getOrElse("VARIANT_BASE", "/tmp/hudiinaction/chapter09")
val morPath  = s"$basePath/variant_mor/agent_events_mor"

println(s"spark.version = ${spark.version}")

def readMorSnapshot(): org.apache.spark.sql.DataFrame = {
  spark.read.format("hudi").load(morPath)
    .selectExpr("event_id", "payload IS NULL AS payload_is_null", "CAST(payload AS STRING) AS payload_str")
    .orderBy("event_id")
}

println("=== pushVariantIntoScan=true (Spark 4.1 default): snapshot read of MOR table with an uncompacted update ===")
spark.conf.set("spark.sql.variant.pushVariantIntoScan", "true")
readMorSnapshot().show(false)
println("Rows where payload_is_null=true and `IS NULL` correctly caught the nulled-out e3 row (if any):")
spark.read.format("hudi").load(morPath).where("payload IS NULL").select("event_id").show(false)

println("=== workaround: pushVariantIntoScan=false ===")
spark.conf.set("spark.sql.variant.pushVariantIntoScan", "false")
readMorSnapshot().show(false)
println("Rows caught by `payload IS NULL` with the workaround applied:")
spark.read.format("hudi").load(morPath).where("payload IS NULL").select("event_id").show(false)

println("VARIANT_PUSHDOWN_MOR_DONE")
System.exit(0)
