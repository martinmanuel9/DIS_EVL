#!/usr/bin/env python 

"""
Application:        Apache Spark Application 
File name:          pyspark_streaming.py
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
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

# Define the Kafka broker(s) and topic
kafka_bootstrap_servers = "localhost:9092"
kafka_brokers = "kafka-broker1:9092,kafka-broker2:9092"
kafka_topic = "fridge"

# Initialize SparkContext
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

# Define your Kafka input source configuration
kafka_source = spark.readStream \
    .format("kafka") \
    .option(kafka_bootstrap_servers, kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .load()

schema = StructType([StructField("timestamp", TimestampType(), True),
                     StructField("temperature", DoubleType(), True),
                     StructField("temp_condition", StringType(), True),
                     StructField("attack", StringType(), True),
                     StructField("label", IntegerType(),True)])

# Parse the Kafka message value as JSON
parsed_data = kafka_source.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json("json_value", schema).alias("data"))

def write_to_cassandra(batchDF, batchId):
    batchDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "fridge_sim") \
        .option("table", "fridge_data") \
        .mode("append") \
        .save()

query = parsed_data.writeStream \
    .foreachBatch(write_to_cassandra) \
    .start()

query.awaitTermination()