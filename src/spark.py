from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read JSON with Spark") \
    .getOrCreate()

#ToDo Define the path to the JSON file
json_file_path = ""

# Read the JSON file into a DataFrame, specifying the schema
df = spark.read.option('multiline', True).json(json_file_path)

# Show the contents of the DataFrame
df.show()

# Stop the Spark session
spark.stop()
