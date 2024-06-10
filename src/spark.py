from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf
from pyspark.sql.types import ArrayType, DoubleType, StringType
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read JSON with Spark") \
    .getOrCreate()

# Define the path to the JSON file
json_file_path = "/Users/Mick/Desktop/PycharmProjects/DIS/test.json"

# Read the JSON file into a DataFrame, specifying the schema
df = spark.read.option('multiline', True).json(json_file_path)

# Group by ID and collect all relevant columns into a list
grouped_df = df.groupBy("ID").agg(
    collect_list(col("server_1")).alias("servers_1"),
    collect_list(col("server_2")).alias("servers_2"),
    collect_list(col("time_stamp")).alias("time_stamps"),
    collect_list(col("type")).alias("types")
)

# Define a UDF to compute time differences
def compute_time_differences(time_stamps):
    return [round(time_stamps[i] - time_stamps[i-1], ndigits=2) for i in range(1, len(time_stamps))]

# Register the UDF
compute_time_differences_udf = udf(compute_time_differences, ArrayType(DoubleType()))

# Apply the UDF to calculate time differences
result_df = grouped_df.withColumn("time_differences", compute_time_differences_udf(col("time_stamps")))

# Compute the total time as the sum of time differences
result_df = result_df.withColumn("total_time", F.expr("aggregate(time_differences, 0D, (acc, x) -> acc + x)"))

# Define the bucketing criteria
total_time_buckets = [
    (200, 250, "200-250"),
    (250, 300, "250-300"),
    (300, 350, "300-350")
]

# Define a UDF to assign buckets based on total time
def assign_total_time_bucket(total_time):
    for lower, upper, bucket_name in total_time_buckets:
        if lower <= total_time < upper:
            return bucket_name
    return "out_of_range"

# Register the UDF
assign_total_time_bucket_udf = udf(assign_total_time_bucket, StringType())

# Apply the UDF to assign each process to a bucket
bucketed_df = result_df.withColumn("total_time_bucket", assign_total_time_bucket_udf(col("total_time")))

# Show the bucketed DataFrame
bucketed_df.select("ID", "total_time", "total_time_bucket").show(truncate=False)

# Define sub-bucketing criteria based on the length of 'server_1' list
process_length_buckets = [
    (4, 6, "4-6"),
    (7, 9, "7-9")
]

# Define a UDF to assign sub-buckets based on the length of 'server_1' list
def assign_process_length_bucket(servers_1):
    length = len(servers_1)
    for lower, upper, sub_bucket_name in process_length_buckets:
        if lower <= length <= upper:
            return sub_bucket_name
    return "out_of_range"

# Register the UDF
assign_process_length_bucket_udf = udf(assign_process_length_bucket, StringType())

# Apply the UDF to assign each process to a sub-bucket
bucketed_df = bucketed_df.withColumn("process_length_bucket", assign_process_length_bucket_udf(col("servers_1")))

# Combine the buckets into a single column for easier handling
bucketed_df = bucketed_df.withColumn("combined_bucket", F.concat_ws("_", col("total_time_bucket"), col("process_length_bucket")))

# Show the final DataFrame with sub-buckets
bucketed_df.show()

# Stop the Spark session
spark.stop()
