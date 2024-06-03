from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read JSON with Spark") \
    .getOrCreate()

# Define the path to the JSON file
json_file_path = "Users/Mick/Desktop/DIS/function.json"

# Read the JSON file into a DataFrame
df = spark.read.json(json_file_path)

# Show the contents of the DataFrame
df.show()

# Optionally, print the schema of the DataFrame
df.printSchema()

# Stop the Spark session
spark.stop()
