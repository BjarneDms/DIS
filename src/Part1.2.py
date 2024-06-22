import copy
import pickle

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf, stddev as stddev_spark
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import functions as F
import json
from filefunctions import observationfile, output
from filefunctions import process_length, stddev, reactiontime
from collections import Counter
import networkx as nx




# Create a Spark session
spark = SparkSession.builder \
    .appName("Merge identical processes") \
    .getOrCreate()

# Define the path to the JSON file
json_file_path = "../data/logfile.json"

# Read the JSON file into a DataFrame, specifying the schema
df = spark.read.option('multiline', True).json(json_file_path)

# Group by ID and collect all relevant columns into a list
info_df = df.groupBy("ID").agg(
    collect_list(col("server_1")).alias("from_servers"),
    collect_list(col("server_2")).alias("to_servers"),
    collect_list(col("time_stamp")).alias("time_stamps"),
    collect_list(col("type")).alias("types")
)
#Hash Functions
#Hash 1: Total length (Unique server mentions)
def assign_process_length_bucket(from_servers:ArrayType) -> int:
    return process_length(from_servers)
assign_process_length_bucket_udf = udf(assign_process_length_bucket, IntegerType())

#Hash 2: Branching factor
def branching_factor_bucket(types:ArrayType) -> int:
    branching_factor = 0
    for i in range(len(types)):
        v = 0
        if types[i] == 'Response':
            v = 1
        branching_factor += i*v
    return branching_factor
branching_factor_bucket_udf = udf(branching_factor_bucket, IntegerType())

# Hash 3: variance 1 and 2
variance_udf = udf(stddev, IntegerType())
reactiontime_udf = udf(reactiontime, ArrayType(IntegerType()))
info_df = info_df.withColumn(colName="reactiontime", col=reactiontime_udf(col("time_stamps")))
info_df = info_df.withColumn(colName="variance", col=variance_udf(col("reactiontime")))

# Determine the size of the buckets
stddev = info_df.select(stddev_spark("variance")).collect()[0][0]
# print(f"std: {stddev}")
#sum_std = info_df.agg({"variance": "sum"}).collect()[0][0]
#count_std = info_df.count()
#stdvar = sum_std / count_std
#print(stdvar)
bucket_factor = 0.0000005
bucket_size = stddev / bucket_factor
print(bucket_size)

def variancehash_1(variance) -> int:
    return int(variance // bucket_size + 1)

def variancehash_2(variance) -> int:
    return int((variance - bucket_size + 1) // bucket_size + 1)

variancehash_1_udf = udf(variancehash_1, IntegerType())
variancehash_2_udf = udf(variancehash_2, IntegerType())

# Apply Hash 1:
info_df = info_df.withColumn("process_length_bucket", assign_process_length_bucket_udf(col("from_servers")))

#Apply Hash 2:
info_df = info_df.withColumn("branching_factor_bucket", branching_factor_bucket_udf(col("types")))

#Apply Hash 3:
info_df = info_df.withColumn("variance1_bucket", variancehash_1_udf(col("variance")))
info_df = info_df.withColumn("variance2_bucket", variancehash_2_udf(col("variance")))

# Combine buckets into a column
combined_bucket_df = info_df.withColumn("combined_bucket1",
    F.concat_ws("_",  col("process_length_bucket"),
    col("branching_factor_bucket"), col("variance1_bucket")))
combined_bucket_df = combined_bucket_df.withColumn("combined_bucket2",
    F.concat_ws("_",  col("process_length_bucket"),
    col("branching_factor_bucket"), col("variance2_bucket"))).select("ID", "combined_bucket1", "combined_bucket2")

# Combine buckets without variance, used in experiment 1
exp_bucket_df = info_df.withColumn("exp_bucket",
    F.concat_ws("_",  col("process_length_bucket"),
    col("branching_factor_bucket"))).select("ID", "exp_bucket")

# Group and combine by the 2 combined buckets and collect the IDs
bucket1_to_ids_df = combined_bucket_df.groupBy("combined_bucket1").agg(
    collect_list("ID").alias("process_ids"))
bucket2_to_ids_df = combined_bucket_df.groupBy("combined_bucket2").agg(
    collect_list("ID").alias("process_ids"))
bucket_to_ids_df = bucket1_to_ids_df.union(bucket2_to_ids_df).distinct()

# Group the bucket for experiment 1
exp_bucket_to_ids_df = exp_bucket_df.groupBy("exp_bucket").agg(
    collect_list("ID").alias("process_ids"))

# Show the DataFrame
bucket_to_ids_df.show()
exp_bucket_to_ids_df.show()
info_df.show(truncate=False)

#----------------------------------------------------------------------------------------------------------------------------------------------------------#
"""
In this section we make a dictionary with the groups that should be merged
assuming transitivity
"""
jac_treshold = 0.5
'''
merge_dict = {}
info_collected = info_df.collect()
for row in info_collected:
    id = row['ID']
    merge_dict[id] = []
buckets_collected = bucket_to_ids_df.collect()
for bucket in buckets_collected:
    ids = bucket['process_ids']
    for i in range(len(ids)):
        for j in range(i+1, len(ids)):
            if ids[j] not in merge_dict[ids[i]]:
                servers_i = info_df.filter(col("ID") == ids[i]).select("to_servers").collect()[0]['to_servers']
                servers_j = info_df.filter(col("ID") == ids[j]).select("to_servers").collect()[0]['to_servers']
                counter_i = Counter(servers_i)
                counter_j = Counter(servers_j)

                intersection_count = sum((counter_i & counter_j).values())
                union_count = sum((counter_i | counter_j).values())
                jac_sim = float(intersection_count) / union_count

                if jac_sim > jac_treshold:
                    merge_dict[ids[i]].append(ids[j])
'''


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

            servers_i = info_df.filter(col("ID") == process_id_1).select("to_servers").collect()[0]['to_servers']
            servers_j = info_df.filter(col("ID") == process_id_2).select("to_servers").collect()[0]['to_servers']
            counter_i = Counter(servers_i)
            counter_j = Counter(servers_j)

            #intersection = counter_i.intersection(counter_j)
            #union = counter_i.union(counter_j)
            #jac_sim = len(intersection) / len(union) if len(union) > 0 else 0

            intersection_count = sum((counter_i & counter_j).values())
            union_count = sum((counter_i | counter_j).values())             # - intersection_count
            if union_count > 0:
                jac_sim = float(intersection_count) / union_count
            else:
                jac_sim = 1

            if jac_sim > jac_treshold and not G.has_edge(process_id_1, process_id_2):
                G.add_edge(process_id_1, process_id_2)

merge_lists = list(nx.connected_components(G))
clusters = dict()
key = 0
for merge_list in merge_lists:
    clusters[key] = list(merge_list)
    key += 1
print(clusters)
#-----------------------------------------------------------------------------------------------------------------------------#
"""
Next we write the output and observation files
"""

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

# Create list of candidate pairs for experiment 1
cp_dict = {}
collected_data = exp_bucket_to_ids_df.collect()
key = 0
for row in collected_data:
    ids = row['process_ids']
    if len(ids)>1:
        cp_dict[key] = ids
        key += 1
with open("../data/length_branch_buckets.json", 'w') as f:
    json.dump(cp_dict, f)

clusters_deepcopy = copy.deepcopy(clusters)
with open('clusters1.pkl', 'wb') as f:
    pickle.dump(clusters_deepcopy, f)

# Stop the Spark session
spark.stop()
