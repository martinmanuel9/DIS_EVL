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

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark_cassandra import streaming
import json

# Initialize SparkContext
sc = SparkContext(appName="KafkaToCassandraStreaming")

# Initialize StreamingContext
ssc = StreamingContext(sc, batchDuration=5)  # Adjust batchDuration as needed

# Connect to Kafka (replace kafka_bootstrap_servers and kafka_topic)
kafka_stream = KafkaUtils.createStream(
    ssc,
    "localhost:9092", # Specify the Kafka bootstrap servers
    "dis", # Specify a consumer group
    {"fridge": 1}  # Specify the Kafka topic and the number of threads
)

# Define a function to process each RDD (batch) of messages
def process_message(rdd):
    # Example: Deserialize JSON messages and save to Cassandra
    rdd.map(lambda x: json.loads(x[1])) \
       .foreachRDD(lambda rdd: rdd.saveToCassandra("my_keyspace", "fridgedata"))

# Process and save messages
kafka_stream.foreachRDD(process_message)

# Start the streaming context
ssc.start()
ssc.awaitTermination()
