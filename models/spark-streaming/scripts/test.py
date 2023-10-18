
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType
import uuid

BOOTSTRAP_SERVERS = "localhost:9092"

# Create a Spark session
spark = SparkSession \
    .builder \
    .appName("Spark Kafka Streaming Data Pipeline") \
    .config("spark.cassandra.connection.host", "172.18.0.5") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define the schema for the Kafka message
fridgeSchema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("temp_condition", StringType(), True),
    StructField("attack", StringType(), True),
    StructField("label", StringType(), True)
])

# Create a Kafka DataFrame for streaming
input_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "fridge") \
    .option("startingOffsets", "earliest") \
    .load()

# Extract and parse the message value
expanded_df = input_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), fridgeSchema).alias("order")) \
    .select("order.*")

# Generate a UUID column
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
expanded_df = expanded_df.withColumn("uuid", uuid_udf())

# Output to Console
console_query = expanded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Define a checkpoint location for the streaming query
checkpoint_location = "path_to_checkpoint_directory"

# Output to Cassandra
cassandra_query = expanded_df.writeStream \
    .trigger(processingTime="15 seconds") \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_location) \
    .foreachBatch(save_to_cassandra) \
    .start()

# Output to MySQL
mysql_query = expanded_df.writeStream \
    .trigger(processingTime="15 seconds") \
    .outputMode("complete") \
    .option("checkpointLocation", checkpoint_location) \
    .foreachBatch(save_to_mysql) \
    .start()

# Wait for the streaming queries to terminate
console_query.awaitTermination()
cassandra_query.awaitTermination()
mysql_query.awaitTermination()
