// Chapter 9 encryption example, part 2: a FRESH spark-shell session (no crypto Hadoop
// config set at all) tries to read the COW table encryption_cow.scala just wrote.
// (b) This should fail for the encrypted column.

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/encryption")
val cowPath = s"$basePath/cow_rides"

println("=== (b) Reading the encrypted COW table in a FRESH session WITHOUT crypto config ===")
try {
  spark.read.format("hudi").load(cowPath).select("id", "rider", "city", "fare").orderBy("id").show(false)
  println("Read WITHOUT keys SUCCEEDED (unexpected -- fare should have been unreadable).")
} catch {
  case e: Throwable =>
    println(s"Read WITHOUT keys FAILED as expected.")
    println(s"Exception class:   ${e.getClass.getName}")
    println(s"Exception message: ${e.getMessage}")
    var cause = e.getCause
    while (cause != null) {
      println(s"  caused by: ${cause.getClass.getName}: ${cause.getMessage}")
      cause = cause.getCause
    }
}

// A read that never touches the encrypted column should still work, to isolate that the
// failure above is specific to the encrypted column and not the whole file.
println("\n=== Reading only NON-encrypted columns in the same keyless session ===")
try {
  spark.read.format("hudi").load(cowPath).select("id", "rider", "city").orderBy("id").show(false)
  println("Read of non-encrypted columns SUCCEEDED.")
} catch {
  case e: Throwable =>
    println(s"Read of non-encrypted columns FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

System.exit(0)
