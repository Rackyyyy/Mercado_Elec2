from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("AnxietyAnalysis").getOrCreate()

# Load CSV Dataset
df = spark.read.csv("anxiety_attack_dataset.csv", header=True, inferSchema=True)

# Show Schema & First Few Rows
df.printSchema()
df.show(5)

# Partitioning Strategy 1: Partition by Severity Level
df_partitioned_severity = df.repartition("Severity")
print(f"Number of partitions (by Severity): {df_partitioned_severity.rdd.getNumPartitions()}")

# Partitioning Strategy 2: Hash Partitioning (Fixed 4 Partitions)
df_partitioned_fixed = df.repartition(4)
print(f"Number of partitions (Fixed 4 Partitions): {df_partitioned_fixed.rdd.getNumPartitions()}")

# Data Transformations

# 1. Summarize Data: Count of Entries by Severity
df_summary = df.groupBy("Severity").count()
df_summary.show()

# 2. Sort Data by Age
df_sorted = df.orderBy("Age")
df_sorted.show(5)

# 3. Filter Data: Show Only High Severity Cases
df_filtered = df.filter(df["Severity"] == "High")
df_filtered.show(5)

# Save Processed Data to CSV
df_filtered.write.csv("Lab activity/filtered_anxiety_data.csv", header=True, mode="overwrite")

print("Processing complete. Filtered data saved to 'Lab activity/filtered_anxiety_data.csv'.")

# Stop Spark Session
spark.stop()
