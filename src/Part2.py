import copy
import pickle
import json
import networkx as nx
import community
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf, stddev as stddev_spark
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import functions as F
from filefunctions import process_length, stddev, observationfile, reactiontime, jaccard_similarity
from collections import Counter

# Create a spark session
spark = SparkSession.builder \
    .appName("Group similar processes") \
    .getOrCreate()

# Define the filepath
filepath = "../data/part1Output.json"

# Create a dataframe of part1output.json
df = spark.read.option(key='multiline', value=True).json(filepath)
grouped_df = df.groupBy("ID").agg(
    collect_list(col("from_servers")).alias("from_servers"),
    collect_list(col("to_servers")).alias("to_servers"),
    collect_list(col("time_stamp")).alias("time_stamps"),
    collect_list(col("type")).alias("types")
)

# Create the process length and variance functions from filefunctions
length_udf = udf(process_length, IntegerType())
variance_udf = udf(stddev, IntegerType())
reactiontime_udf = udf(reactiontime, ArrayType(IntegerType()))

# Add their columns
df_prep = grouped_df.withColumn(colName="process_length", col=length_udf(col("from_servers")))
df_prep = df_prep.withColumn(colName="reactiontime", col=reactiontime_udf(col("time_stamps")))
df_prep = df_prep.withColumn(colName="variance", col=variance_udf(col("reactiontime")))

# Determine the size of the buckets for length
stddev_l = df_prep.select(stddev_spark("process_length")).collect()[0][0]
bucket_factor_l = 0.5
bucket_size_l = max(int(stddev_l / bucket_factor_l), 1)
print(f'bucket size for length: {bucket_size_l}')

def lengthhash_1(length) -> int:
    return int(length // bucket_size_l + 1)

def lengthhash_2(length):
    return int((length - (bucket_size_l/2)) // bucket_size_l + 1)

# Determine the size of the buckets for variance
stddev_var = df_prep.select(stddev_spark("variance")).collect()[0][0]
bucket_factor_var = 1
bucket_size_var = int(stddev_var/bucket_factor_var)
print(f'bucket size for variance: {bucket_size_var}')


def variancehash_1(variance) -> int:
    return int(variance // bucket_size_var + 1)

def variancehash_2(variance) -> int:
    return int((variance - (bucket_size_var/2)) // bucket_size_var + 1)

# Create the hash functions
lengthhash_1_udf = udf(lengthhash_1, IntegerType())
lengthhash_2_udf = udf(lengthhash_2, IntegerType())
variancehash_1_udf = udf(variancehash_1, IntegerType())
variancehash_2_udf = udf(variancehash_2, IntegerType())

# Hash the new columns and add their bucket keys as columns
all_info_df = df_prep.withColumn(colName="length_1", col=lengthhash_1_udf(col("process_length"))) \
    .withColumn(colName="length_2", col=lengthhash_2_udf(col("process_length"))) \
    .withColumn(colName="variance_1", col=variancehash_1_udf(col("variance"))) \
    .withColumn(colName="variance_2", col=variancehash_2_udf(col("variance")))

# Combine the length hashkeys with the variance hashkeys
lv11_df = all_info_df.withColumn(colName="length1_variance1", col=F.concat_ws("_", col("length_1"), col("variance_1")))
lv12_df = all_info_df.withColumn(colName="length1_variance2", col=F.concat_ws("_", col("length_1"), col("variance_2")))
lv21_df = all_info_df.withColumn(colName="length2_variance1", col=F.concat_ws("_", col("length_2"), col("variance_1")))
lv22_df = all_info_df.withColumn(colName="length2_variance2", col=F.concat_ws("_", col("length_2"), col("variance_2")))

# Show the groups with hashkeys
hashkeys11_df = lv11_df.groupBy("length1_variance1") \
    .agg(collect_list("ID").alias("group_IDs"))
hashkeys12_df = lv12_df.groupBy("length1_variance2") \
    .agg(collect_list("ID").alias("group_IDs"))
hashkeys21_df = lv21_df.groupBy("length2_variance1") \
    .agg(collect_list("ID").alias("group_IDs"))
hashkeys22_df = lv22_df.groupBy("length2_variance2") \
    .agg(collect_list("ID").alias("group_IDs"))

inter1_df = hashkeys11_df.union(hashkeys12_df).distinct()
inter2_df = inter1_df.union(hashkeys21_df).distinct()
finalhashkeys_df = inter2_df.union(hashkeys22_df).distinct()
finalhashkeys_df.show(truncate = False, n = 1000)
all_info_df.orderBy('ID').show(truncate=False, n=1000)

#----------------------------------------------------------------------------------------------------------------------------------------------------------#
"""
In this section a graph will be created with edges between all similar processes
"""
jac_treshold = 0.5

# Create a graph
G = nx.Graph()

# Add nodes and edges to the graph
for row in finalhashkeys_df.collect():
    process_ids = row['group_IDs']
    for i in range(len(process_ids)):
        process_id_1 = process_ids[i]
        G.add_node(process_id_1)
        for j in range(i + 1, len(process_ids)):
            process_id_2 = process_ids[j]
            G.add_node(process_id_2)

            servers_i = set(all_info_df.filter(col("ID") == process_id_1).select("to_servers").collect()[0]['to_servers'])
            servers_j = set(all_info_df.filter(col("ID") == process_id_2).select("to_servers").collect()[0]['to_servers'])
            counter_i = Counter(servers_i)
            counter_j = Counter(servers_j)

            jac_sim = jaccard_similarity(servers_i, servers_j)
            if jac_sim >= jac_treshold and not G.has_edge(process_id_1, process_id_2):
                G.add_edge(process_id_1, process_id_2)

merge_lists = list(nx.connected_components(G))
clusters = dict()
key = 0
for merge_list in merge_lists:
    clusters[key] = list(merge_list)
    key += 1
print(clusters)

# Open the logfile
with open('../data/logfile.json', 'r') as r:
    log = json.load(r)

# Create the outputfile for part 2
observationfile(part="2", group=clusters, logfile=log)

# Create a deepcopy for experiment 2
clusters_deepcopy = copy.deepcopy(clusters)
with open('clusters2.pkl', 'wb') as f:
    pickle.dump(clusters_deepcopy, f)
'''
# Create pkl for experiment 2
with open('../data/part1Output.json', 'r') as r:
    log_merged = 
'''
spark.stop()