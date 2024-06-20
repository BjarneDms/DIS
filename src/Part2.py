import copy
import pickle
import networkx as nx
import community
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import functions as F
from filefunctions import lengthhash_1, lengthhash_2, process_length, stddev, variancehash_1, variancehash_2

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

# Add their columns
df_prep = grouped_df.withColumn(colName="process_length", col=length_udf(col("from_servers"))) \
    .withColumn(colName="variance", col=variance_udf(col("time_stamps")))

# Create the hash functions
lengthhash_1_udf = udf(lengthhash_1, IntegerType())
lengthhash_2_udf = udf(lengthhash_2, IntegerType())
variancehash_1_udf = udf(variancehash_1, IntegerType())
variancehash_2_udf = udf(variancehash_2, IntegerType())

# Hash the new columns and add they bucket keys as columns
all_info_df = df_prep.withColumn(colName="length_1", col=lengthhash_1_udf(col("process_length"))) \
    .withColumn(colName="length_2", col=lengthhash_2_udf(col("process_length"))) \
    .withColumn(colName="variance_1", col=variancehash_1_udf(col("variance"))) \
    .withColumn(colName="variance_2", col=variancehash_2_udf(col("variance")))

# Combine the length hashkeys with the variance hashkeys
lv11_df = all_info_df.withColumn(colName="length1_variance1", col=F.concat_ws("_", col("length_1"), col("variance_1")))
lv12_df =   all_info_df.withColumn(colName="length1_variance2", col=F.concat_ws("_", col("length_1"), col("variance_2")))
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
finalhashkeys_df.show()
# Create a graph with Jaccard similarity as edge weigths
G = nx.Graph()

for row in finalhashkeys_df.collect():
    process_ids = row['group_IDs']
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

            print(servers_i)

            # Jaccard similarity to be assigned as weight to the edge
            jac_sim = float(len(intersection)) / len(union)
            if G.has_edge(process_id_1, process_id_2):
                ()
            else:
                G.add_edge(process_id_1, process_id_2, weight=jac_sim)

# Create cluster using Louvain algorithm as community detection algorithm
partition = community.best_partition(G)
clusters = {v: [k for k, v2 in partition.items() if v2 == v] for v in set(partition.values())}
print(clusters)

# Create a deepcopy for experiment 2
clusters_deepcopy = copy.deepcopy(clusters)
with open('clusters2.pkl', 'wb') as f:
    pickle.dump(clusters_deepcopy, f)

spark.stop()