#!/usr/bin/env python 

"""
Application:        Apache Spark Application 
File name:          pyspark_batch.py
Author:             Martin Manuel Lopez
Creation:           9/27/2023

The University of Arizona
Department of Electrical and Computer Engineering
College of Engineering
"""

# MIT License
#
# Copyright (c) 2023
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Define the Kafka broker(s) and topic
kafka_brokers = "kafka-broker1:9092,kafka-broker2:9092"
kafka_topic = "fridge"


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

# Define a schema for your Kafka messages (adjust as needed)
schema = StructType([StructField("timestamp", TimestampType(), True),
                     StructField("temperature", DoubleType(), True),
                     StructField("temp_condition", StringType(), True),
                     StructField("attack", StringType(), True),
                     StructField("label", IntegerType(),True)])


# Read data from Kafka using the Kafka source and apply the schema
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .load()


# Deserialize the JSON value from Kafka and apply the defined schema
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_value")
kafka_stream = kafka_stream.select(from_json("json_value", schema).alias("data"))


# Extract individual fields from the JSON structure
kafka_stream = kafka_stream.select("data.*")

# Perform some transformations or processing on the Kafka stream
# For example, you can select and display the modified schema
kafka_stream.printSchema()

# Start the streaming query
query = kafka_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()




