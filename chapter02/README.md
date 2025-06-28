# Chapter 2: Hudi Pipeline Quickstart

This chapter provides a comprehensive introduction to Apache Hudi through hands-on examples using the NYC Taxi dataset.

## 🚀 What You'll Learn

This chapter demonstrates:

- **Table Creation**: Setting up Copy-on-Write (CoW) tables
- **Data Loading**: Importing data from CSV files into Hudi tables
- **CRUD Operations**: Performing upserts and deletes on Hudi tables
- **Query Patterns**: Snapshot queries to read the latest data
- **Metadata Exploration**: Understanding commit timeline and table history
- **Table Configuration**: Key Hudi options and their impact

## 📊 Sample Dataset

This chapter uses a sample of the NYC Taxi dataset containing approximately 1 million trip records. The data includes:

- **Trip ID** (unique identifier)
- **Vendor ID** (for partitioning)
- **Pickup/dropoff timestamps**
- **Trip distance and duration**
- **Fare amounts and payment types**
- **Geographic coordinates**

## 🛠️ Setup Instructions

### 1. Extract Sample Data
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
:load hudi_pipeline_quickstart.scala
```

## 🔧 Key Configuration

The tutorial demonstrates these essential Hudi configurations:

- **Record Key**: `trip_id` (unique identifier for each record)
- **Partition Field**: `vendor_id` (distributes data across partitions)
- **Precombine Field**: `pickup_datetime` (resolves duplicates - latest wins)
- **Table Type**: Copy-on-Write (CoW) for optimal read performance
- **Hive Style Partitioning**: Enabled for compatibility

## 📝 Tutorial Steps

### Section 1: Data Loading and Table Creation
- Load NYC taxi data from CSV
- Create your first Hudi CoW table
- Configure essential Hudi options

### Section 2: Upsert Operations
- Update existing records (fare amount increase)
- Modify timestamps (pickup time adjustment)
- Verify changes with snapshot queries

### Section 3: Delete Operations
- Remove records based on conditions
- Perform hard deletes from the table
- Validate deletion results

### Section 4: Metadata Exploration
- Access Hudi table metadata
- Explore commit timeline
- Understand table history

## 🎯 Expected Outcomes

After completing this chapter, you'll have:

- A functioning Hudi table with ~1M records
- Experience with upsert and delete operations
- Understanding of Hudi's metadata structure
- Knowledge of essential configuration options
- Hands-on experience with Spark-Hudi integration

## 📁 Files in This Chapter

- `hudi_pipeline_quickstart.scala` - Complete tutorial script with detailed comments
- `trips_0.gz` - NYC Taxi dataset sample (compressed)
- `README.md` - This chapter guide

## 💡 Tips for Success

1. **Memory Settings**: Ensure Spark has at least 4GB RAM allocated
2. **Path Configuration**: Use absolute paths to avoid confusion
3. **Package Versions**: Match Hudi bundle version with your Spark version
4. **Data Location**: Extract the dataset before running the tutorial
5. **Iterative Learning**: Run sections incrementally to understand each concept

## 🐛 Troubleshooting

**Common Issues:**
- **File Not Found**: Ensure trips_0.gz is extracted to the correct path
- **Memory Errors**: Increase Spark driver memory with `--driver-memory 4g`
- **Package Conflicts**: Use the exact Hudi bundle version for your Spark version
- **Permission Errors**: Ensure write permissions to the basePath directory

## 📚 Further Reading

- [Apache Hudi Documentation](https://hudi.apache.org/)
- [Hudi Configuration Guide](https://hudi.apache.org/docs/configurations/)
- [Spark-Hudi Integration](https://hudi.apache.org/docs/quick-start-guide/) 