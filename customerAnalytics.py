# Install PySpark if not already installed


from pyspark.sql import SparkSession

# Initialize SparkSession with various memory configurations
spark = SparkSession.builder \
    .appName("QuickCommerceCustomerAnalysis") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "1g") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

# Set GC options using Hadoop Configuration
spark._jsc.hadoopConfiguration().set("spark.executor.extraJavaOptions", "-Xloggc:/path/to/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps")
spark._jsc.hadoopConfiguration().set("spark.driver.extraJavaOptions", "-Xloggc:/path/to/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps")

# Load customer data (assuming the CSV file is in the same directory as the notebook)
df = spark.read.csv("customer_data.csv", header=True, inferSchema=True)

# Cache the data for faster access
df.cache()

# Show schema and a few rows to understand the data
df.printSchema()
df.show(5)

# Data Partitioning
df_repartitioned = df.repartition(8)  # Adjust the number of partitions based on the size of your data

# Check the number of partitions
print(f"Number of partitions: {df_repartitioned.rdd.getNumPartitions()}")

# Perform Analysis
df_analysis = df_repartitioned.groupBy("customer_id").agg({"purchase_amount": "avg"})

# Show results
df_analysis.show()

# Disk and Memory Optimization

## Optimize Disk I/O by using Efficient Formats
# Write data to Parquet format for optimized performance
df_repartitioned.write.parquet("customer_data_parquet")

# Read back the data from Parquet format
df_parquet_read = spark.read.parquet("customer_data_parquet")

# Perform the same analysis on Parquet data
df_parquet_analysis = df_parquet_read.groupBy("customer_id").agg({"purchase_amount": "avg"})
df_parquet_analysis.show()

## Optimize Memory Usage by controlling the number of tasks
# Set the number of shuffle partitions (default is 200)
spark.conf.set("spark.sql.shuffle.partitions", "50")

# Check the setting
print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# Perform another analysis to see the effect
df_analysis_optimized = df_parquet_read.groupBy("customer_id").agg({"purchase_amount": "avg"})
df_analysis_optimized.show()
