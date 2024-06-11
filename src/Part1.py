import networkx as nx
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import functions as F
import community
import math
import json
from filefunctions import observationfile


# Create a Spark session
spark = SparkSession.builder \
    .appName("Read JSON with Spark") \
    .getOrCreate()

# Define the path to the JSON file
json_file_path = "../test.json"

# Read the JSON file into a DataFrame, specifying the schema
df = spark.read.option('multiline', True).json(json_file_path)

# Group by ID and collect all relevant columns into a list
grouped_df = df.groupBy("ID").agg(
    collect_list(col("server_1")).alias("servers_1"),
    collect_list(col("server_2")).alias("servers_2"),
    collect_list(col("time_stamp")).alias("time_stamps"),
    collect_list(col("type")).alias("types")
)
#Hash Functions
#Hash 1: Total length (Unique server mentions)
def assign_process_length_bucket(servers_1:ArrayType) -> list:
    return [len(set(servers_1))]
assign_process_length_bucket_udf = udf(assign_process_length_bucket, ArrayType(IntegerType()))

#Hash 2: Total time (in milliseconds)
def compute_time_differences(time_stamps:ArrayType) -> list:
    total_time = round(time_stamps[-1] - time_stamps[0], ndigits=2)
    return [int((total_time // 50) + 1)]
compute_time_differences_udf = udf(compute_time_differences, ArrayType(IntegerType()))

#Hash 3: Variance in time (standard deviation(stddev)
#First define a function to calculate the stddev
def stddev(time_stamps:ArrayType) -> float:
    length = len(time_stamps)
    mean = sum(time_stamps) / length
    var = sum((elem - mean) ** 2 for elem in time_stamps) / length
    var = round(var, ndigits=2)
    return math.sqrt(var)

#Define the hash function to hash to intervals of 5ms
def time_variance_bucket(time_stamps:ArrayType) -> list:
    return [int((stddev(time_stamps) // 5) + 1)]
time_variance_bucket_udf = udf(time_variance_bucket, ArrayType(IntegerType()))

# Apply Hash 1:
df_h1 = grouped_df.withColumn("process_length_bucket", assign_process_length_bucket_udf(col("servers_1")))

#Apply Hash 2:
df_h2 = df_h1.withColumn("total_time_bucket", compute_time_differences_udf(col("time_stamps")))

#Apply Hash 3:
df_h3 = df_h2.withColumn("stddev_bucket", time_variance_bucket_udf(col("time_stamps")))

# Combine buckets into a column
#process length & total time
length_time_bucket_df = df_h3.withColumn("length_time_bucket", F.concat_ws("_",  col("process_length_bucket"), col("total_time_bucket")))

#process length & stddev
length_stddev_bucket_df = length_time_bucket_df.withColumn("length_stddev_bucket", F.concat_ws("_", col("process_length_bucket"), col("stddev_bucket")))

# The DataFrame that keeps all information per process (ID)
all_info_df = length_stddev_bucket_df.select("ID", "servers_1", "servers_2", "time_stamps", "types", "length_time_bucket", "length_stddev_bucket")

# Group by the 2 combined buckets and collect the IDs
bucket_to_ids_df = length_stddev_bucket_df.groupBy("length_time_bucket", "length_stddev_bucket").agg(
    collect_list("ID").alias("process_ids")
)
# Show the DataFrame
bucket_to_ids_df.show()

#----------------------------------------------------------------------------------------------------------------------------------------------------------#
"""
In this section a graph will be created between all candidate processes.
The nodes represent the processes
The edges the candidate pairs
The edge weigths are the Jaccard similarities
"""

# Create a graph
G = nx.Graph()

# Add nodes and edges to the graph
for row in bucket_to_ids_df.collect():
    process_ids = row['process_ids']
    print(process_ids)
    for i in range(len(process_ids)):
        process_id_1 = process_ids[i]
        G.add_node(process_id_1)
        for j in range(i + 1, len(process_ids)):
            process_id_2 = process_ids[j]
            print(process_id_1, process_id_2)
            G.add_node(process_id_2)

            servers_i = all_info_df.filter(col("ID") == process_id_1).select("servers_2").collect()[0]['servers_2']
            servers_j = all_info_df.filter(col("ID") == process_id_2).select("servers_2").collect()[0]['servers_2']
            union = set(servers_i) | set(servers_j)
            intersection = set(servers_i) & set(servers_j)

            # Jaccard similarity to be assigned as weight to the edge
            jac_sim = float(len(intersection)) / len(union)
            if G.has_edge(process_id_1, process_id_2):
                ()
            else:
                G.add_edge(process_id_1, process_id_2, weight=jac_sim)

#-----------------------------------------------------------------------------------------------------------------------------#
"""
From the network, equivalence clusters are created using the Louvain algorithm
for weighted graphs
"""
# Use the Louvain algorithm to find clusters
partition = community.best_partition(G)

#Switch the key-value pairs so each key denotes a cluster of processes
clusters = {v: [k for k, v2 in partition.items() if v2 == v] for v in set(partition.values())}

# # Write the results to the observation file
# Open the logfile
with open('../test.json', 'r') as r:
    logfile = json.load(r)

# Write to .txt
observationfile(part="1", group=clusters, logfile=logfile)

# Stop the Spark session
spark.stop()