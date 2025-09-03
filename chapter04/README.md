# Chapter 3: Hudi tutorial on various querying capabilities with Hudi tables

This chapter covers various querying capabilities with Hudi table and we will be using TPC-DS store_sales dataset. 

## 🚀 What You'll Learn

This chapter demonstrates:

- **Creating a Hudi table**: Initial loading of data
- **Querying Hudi table using spark datasource**: Reading Hudi table from datasource
- **Querying Hudi table using spark SQL**: Reading Hudi table from spark SQL
- **Querying Hudi table using spark structured streaming**: Reading a Hudi table using spark streaming.

## 📊 Sample Dataset

This chapter uses a sample of the store-sales dataset from tpc-ds benchmarking containing approximately 2.8M records. The data includes:

- **ss_item_sk** (unique identifier)
- **Item information** 
- **Customer information**
- **Address related info**
- **Price related stats**
- **Profit related stats**

## 🛠️ Setup Instructions

### 1. Update Configuration
Edit the Scala file to update paths according to your environment:
```scala
// Update these paths in hudi_querying_tutorial.scala
val inputPath = "/path/to/hudiinaction/chapter04/input_data/"
val basePath  = "/tmp/trips_table"  // Or your preferred location
```

### 3. Start Spark Shell
```bash
spark-shell --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
            --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
            --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
            --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'
```

### 4. Run the Tutorial

Execute each read capability from hudi_querying_tutorial interactively by inspecting the output after each operation.

## 📝 Tutorial Steps

### Section 1: Data Loading with bulk_insert
- Load store_sales dataset from input_data
- Bulk import entire data into Hudi table

### Section 2: Querying tables via spark datasource
- Query the hudi table using spark datasource. 

### Section 3: Querying hudi table via Spark SQL
- Create external table from spark SQL
- Query the hudi table from spark SQL

### Section 4: Querying hudi table via Spark Structured Streaming
- Read the hudi table using dstream and process each micro batches to print some stats.

## 🎯 Expected Outcomes

After completing this chapter, you'll have:

- A functioning Hudi table with store_sales dataset. 
- Experience how to query the table via spark datasource. 
- Experience how to querying an external table via spark SQL.
- Explore how to delete data with Hudi
- Learn how to do a streaming read of a given hudi table in micro batches.

## 📁 Files in This Chapter

- `hudi_querying_tutorial.scala` - Complete tutorial script with detailed comments
- `input_data` - data directory containing input data to play with.
- `README.md` - This chapter guide

## 📚 Further Reading

- [Apache Hudi Documentation](https://hudi.apache.org/)
- [Hudi Configuration Guide](https://hudi.apache.org/docs/configurations/)
- [Spark-Hudi Integration](https://hudi.apache.org/docs/quick-start-guide/) 