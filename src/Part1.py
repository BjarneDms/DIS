import copy
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import functions as F
import networkx as nx
import community
import json
from filefunctions import stddev, observationfile, output
from filefunctions import process_length, stddev



# Create a Spark session
spark = SparkSession.builder \
    .appName("Merge identical processes") \
    .getOrCreate()

# Define the path to the JSON file
json_file_path = "../data/logfile.json"

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
    return [process_length(from_servers)]
assign_process_length_bucket_udf = udf(assign_process_length_bucket, ArrayType(IntegerType()))

#Hash 2: Branching factor
def branching_factor_bucket(types:ArrayType) -> list:
    branching_factor = 0
    for i in range(len(types)):
        v = 0
        if types[i] == 'Response':
            v = 1
        branching_factor += i*v
    return [branching_factor]
branching_factor_bucket_udf = udf(branching_factor_bucket, ArrayType(IntegerType()))

# Hash 3: variance 1 and 2
variance_udf = udf(stddev, IntegerType())
grouped_df = grouped_df.withColumn(colName="variance", col=variance_udf(col("time_stamps")))

def variancehash_1(variance) -> int:
    return (variance // 10) + 1

def variancehash_2(variance) -> int:
    return ((variance - 5) // 10) +1

variancehash_1_udf = udf(variancehash_1, IntegerType())
variancehash_2_udf = udf(variancehash_2, IntegerType())

# Apply Hash 1:
df_h1 = grouped_df.withColumn("process_length_bucket", assign_process_length_bucket_udf(col("from_servers")))

#Apply Hash 2:
df_h2 = df_h1.withColumn("branching_factor_bucket", branching_factor_bucket_udf(col("types")))

#Apply Hash 3:
df_h3a = df_h2.withColumn("variance1_bucket", variancehash_1_udf(col("variance")))
df_h3 = df_h3a.withColumn("variance2_bucket", variancehash_2_udf(col("variance")))

# Combine buckets into a column
combined_bucket_a_df = df_h3.withColumn("combined_bucket1",
    F.concat_ws("_",  col("process_length_bucket"),
    col("branching_factor_bucket"), col("variance1_bucket")))
combined_bucket_df = combined_bucket_a_df.withColumn("combined_bucket2",
    F.concat_ws("_",  col("process_length_bucket"),
    col("branching_factor_bucket"), col("variance2_bucket")))
combined_bucket_a_df.show()
combined_bucket_df.show()

# The DataFrame that keeps all information per process (ID)
all_info_df = combined_bucket_df.select("ID", "from_servers", "to_servers", "time_stamps", "types", "combined_bucket1", "combined_bucket2")

# Group and combine by the 2 combined buckets and collect the IDs
bucket1_to_ids_df = combined_bucket_df.groupBy("combined_bucket1").agg(
    collect_list("ID").alias("process_ids"))
bucket2_to_ids_df = combined_bucket_df.groupBy("combined_bucket2").agg(
    collect_list("ID").alias("process_ids"))
bucket_to_ids_df = bucket1_to_ids_df.union(bucket2_to_ids_df).distinct()
# Show the DataFrame

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

deepcopy_pickle = copy.deepcopy(clusters)
with open('clusters1.pkl', 'wb') as f:
    pickle.dump(deepcopy_pickle, f)

# Stop the Spark session
spark.stop()
