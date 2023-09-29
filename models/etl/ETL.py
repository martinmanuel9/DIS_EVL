#!/usr/bin/env python 

"""
Application:        Apache Spark ETL  
File name:          ETL.py
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
from pyspark.sql import SparkSession
import json

def process_message(rdd):
    # Define a function to process each RDD (batch) of messages
    # In this function, you can transform and save the data to Cassandra

    # Example transformation: Parse JSON messages
    parsed_rdd = rdd.map(lambda x: json.loads(x[1]))

    # Example: Save data to Cassandra
    parsed_rdd.saveToCassandra("my_keyspace", "table_name")

if __name__ == "__main__":
    # Initialize Spark
    sc = SparkContext(appName="KafkaToCassandra")
    ssc = StreamingContext(sc, batchDuration=5)  # Adjust batchDuration as needed

    # Connect to Kafka
    kafka_stream = KafkaUtils.createStream(
        ssc,
        "zookeeper_quorum",
        "consumer_group",
        {"kafka_topic": 1}  # Specify the Kafka topic and the number of threads
    )

    # Process and save messages
    kafka_stream.foreachRDD(process_message)

    # Start the streaming context
    ssc.start()
    ssc.awaitTermination()
