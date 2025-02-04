from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("AnxietyAnalysis").getOrCreate()

# Load CSV Dataset
df = spark.read.csv("anxiety_attack_dataset.csv", header=True, inferSchema=True)

# Show Schema & First Few Rows
df.printSchema()
df.show(5)

# Partitioning Strategy 1: Partition by Severity Level
# Use the correct column name 'Severity of Anxiety Attack (1-10)'
df_partitioned_severity = df.repartition("Severity of Anxiety Attack (1-10)")
print(f"Number of partitions (by Severity): {df_partitioned_severity.rdd.getNumPartitions()}")

# Partitioning Strategy 2: Hash Partitioning (Fixed 4 Partitions)
df_partitioned_fixed = df.repartition(4)
print(f"Number of partitions (Fixed 4 Partitions): {df_partitioned_fixed.rdd.getNumPartitions()}")

# Data Transformations

# 1. Summarize Data: Count of Entries by Severity (use the correct severity column)
# Group by 'Severity of Anxiety Attack (1-10)' instead of 'Severity'
df_summary = df.groupBy("Severity of Anxiety Attack (1-10)").count()
df_summary.show()

# 2. Sort Data by Age
df_sorted = df.orderBy("Age")
df_sorted.show(5)

# 3. Filter Data: Show Only High Severity Cases
# Make sure to filter based on the correct severity column, 'Severity of Anxiety Attack (1-10)'
df_filtered = df.filter(df["Severity of Anxiety Attack (1-10)"] == 10)  # Assuming 10 is high severity
df_filtered.show(5)

# Print column names
print(df.columns)

# Save Processed Data to CSV
# Use the correct path syntax (escaping spaces)
df_filtered.write.csv("Lab activity/filtered_anxiety_data.csv", header=True, mode="overwrite")

print("Processing complete. Filtered data saved to 'Lab activity/filtered_anxiety_data.csv'.")

# Stop Spark Session
spark.stop()
