// Follow-up 1 part 2: fresh session, NO crypto config, read the plain-Spark-written file.
val basePath = "/tmp/hudiinaction/chapter09/encryption_control"
println("=== Reading the plain-Spark encrypted parquet WITHOUT keys, fresh session ===")
try {
  spark.read.parquet(basePath).select("id","rider","city","fare").orderBy("id").show(false)
  println("Read WITHOUT keys SUCCEEDED (unexpected).")
} catch {
  case e: Throwable =>
    println(s"Read WITHOUT keys FAILED as expected: ${e.getClass.getName}: ${e.getMessage}")
}
System.exit(0)
