# Chapter 2: Hudi tutorial on various write operations

This chapter covers various write operations with Apache Hudi through hands-on examples using the NYC Taxi dataset.

## 🚀 What You'll Learn

This chapter demonstrates:

- **Bulk data loading**: Initial bulk loading of data
- **Ingesting Immutable data**: Ingesting immutable data into Hudi tables
- **Upsert operation**: Performing upserts on Hudi tables
- **Deleting data**: Deleting data with Hudi
- **Insert overwrite operations**: Performing Insert_Overwrite operations for overwriting entire partitions or table
- **Deleting partitions**: Deleting entire partitions

## 📊 Sample Dataset

This chapter uses a sample of the NYC Taxi dataset containing approximately 1 million trip records. The data includes:

- **Trip ID** (unique identifier)
- **Vendor ID** (for partitioning)
- **Pickup/dropoff timestamps**
- **Trip distance and duration**
- **Fare amounts and payment types**
- **Geographic coordinates**

## 🛠️ Setup Instructions

### 1. Extract Sample Data from chapter2 location
```bash
cd chapter02
gunzip trips_0.gz
```

### 2. Update Configuration
Edit the Scala file to update paths according to your environment:
```scala
// Update these paths in hudi_pipeline_quickstart.scala
val inputPath = "/path/to/hudiinaction/chapter02/trips_0"
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
```scala
:load hudi_ingestion_tutorial.scala
```

## 🔧 Key Configuration

The tutorial demonstrates these essential Hudi configurations:

- **Record Key**: `trip_id` (unique identifier for each record)
- **Partition Field**: `vendor_id` (distributes data across partitions)
- **Table Type**: Copy-on-Write (CoW) for optimal read performance
- **Hive Style Partitioning**: Enabled for compatibility

## 📝 Tutorial Steps

### Section 1: Data Loading with bulk_insert
- Load NYC taxi data from CSV
- Bulk import entire data into Hudi table
- Configure essential Hudi options

### Section 2: Ingesting Immutable data via Insert operation
- Ingest immutable data via insert operation to auto manage small files

### Section 3: Upsert Operations
- Execute upsert operation with Hudi to ingest both inserts and updates.

### Section 3: Delete Operation
- Execute delete operation with Hudi to delete some data.

### Section 3: Insert_Overwrite_Table Operations
- Execute insert_overwrite_table operation with Hudi to overwrite entire table with new data.

### Section 3: Insert_Overwrite Operations
- Execute insert_Overwrite operation with Hudi to overwrite matching partitions.

### Section 3: Delete_Partition Operations
- Execute delete_partition operation with Hudi to delete entire partitions.

## 🎯 Expected Outcomes

After completing this chapter, you'll have:

- A functioning Hudi table with ~1M records
- Experience how to ingest immutable data into Hudi
- Experience how to ingest both inserts and updates in the same batch 
- Explore how to delete data with Hudi
- Learn how to perform insert_overwrite operations with Hudi 
- Learn how to delete entire partitions with Hudi

## 📁 Files in This Chapter

- `hudi_ingstion_tutorial.scala` - Complete tutorial script with detailed comments
- `trips_0.gz` - NYC Taxi dataset sample (compressed)
- `README.md` - This chapter guide

## 💡 Tips for Success

1. **Memory Settings**: Ensure Spark has at least 4GB RAM allocated
2. **Path Configuration**: Use absolute paths to avoid confusion
3. **Package Versions**: Match Hudi bundle version with your Spark version
4. **Data Location**: Extract the dataset before running the tutorial
5. **Iterative Learning**: Run sections incrementally to understand each concept

## 📚 Further Reading

- [Apache Hudi Documentation](https://hudi.apache.org/)
- [Hudi Configuration Guide](https://hudi.apache.org/docs/configurations/)
- [Spark-Hudi Integration](https://hudi.apache.org/docs/quick-start-guide/) 