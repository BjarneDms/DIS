from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import functions as F
import networkx as nx
import community
import json
from filefunctions import stddev, observationfile, output


# Create a Spark session
spark = SparkSession.builder \
    .appName("Merge identical processes") \
    .getOrCreate()

# Define the path to the JSON file
json_file_path = "../test.json"

# Read the JSON file into a DataFrame, specifying the schema
df = spark.read.option('multiline', True).json(json_file_path)

# Group by ID and collect all relevant columns into a list
grouped_df = df.groupBy("ID").agg(
    collect_list(col("server_1")).alias("from_servers"),
    collect_list(col("server_2")).alias("to_servers"),
    collect_list(col("time_stamp")).alias("time_stamps"),
    collect_list(col("type")).alias("types")
)
#Hash Functions
#Hash 1: Total length (Unique server mentions)
def assign_process_length_bucket(from_servers:ArrayType) -> list:
    return [len(set(from_servers))]
assign_process_length_bucket_udf = udf(assign_process_length_bucket, ArrayType(IntegerType()))

#Hash 2: Total time (in milliseconds)
def compute_time_differences(time_stamps:ArrayType) -> list:
    total_time = round(time_stamps[-1] - time_stamps[0], ndigits=2)
    return [int((total_time // 50) + 1)]
compute_time_differences_udf = udf(compute_time_differences, ArrayType(IntegerType()))

#Hash 3: Variance in time (5ms)(standard deviation(stddev)
def time_variance_bucket(time_stamps:ArrayType) -> list:
    return [int((stddev(time_stamps) // 5) + 1)]
time_variance_bucket_udf = udf(time_variance_bucket, ArrayType(IntegerType()))

# Apply Hash 1:
df_h1 = grouped_df.withColumn("process_length_bucket", assign_process_length_bucket_udf(col("from_servers")))

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
all_info_df = length_stddev_bucket_df.select("ID", "from_servers", "to_servers", "time_stamps", "types", "length_time_bucket", "length_stddev_bucket")

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
    for i in range(len(process_ids)):
        process_id_1 = process_ids[i]
        G.add_node(process_id_1)
        for j in range(i + 1, len(process_ids)):
            process_id_2 = process_ids[j]
            G.add_node(process_id_2)

            servers_i = all_info_df.filter(col("ID") == process_id_1).select("to_servers").collect()[0]['to_servers']
            servers_j = all_info_df.filter(col("ID") == process_id_2).select("to_servers").collect()[0]['to_servers']
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
# # Write the results to the output & observation file
# Open the logfile
with open('../data/logfile.json', 'r') as r:
    log = json.load(r)

# Write output to .txt
output(group=clusters, logfile=log)

# And to .json for further analysis
formatted_data = []
with open("../data/part1Output.txt", 'r') as r:
    for line in r:
        strip = line.strip()
        clean = strip.replace('<', '').replace('>', '').split(",")
        call = {f"from_servers": clean[0], f"to_servers": clean[1], f"time_stamp": float(clean[2]), f"type": clean[3], f"ID": int(clean[4])}
        formatted_data.append(call)

with open("../data/part1Output.json", 'w') as f:
    json.dump(formatted_data, f, indent=4)

# Write observations to .txt
observationfile(part="1", group=clusters, logfile=log)

# Stop the Spark session
spark.stop()
