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
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, StringType, IntegerType

# Define the Kafka broker(s) and topic
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "fridge"
CHECKPOINT_LOCATION = "/home/martinmlopez/DIS_EVL/models/etl"

if __name__ == "__main__":

    # Initialize SparkSession
    spark = (
        SparkSession.builder
        .appName("KafkaStructuredStreaming")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Define Kafka source options
    kafka_source_options = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "subscribe": KAFKA_TOPIC,
        "startingOffsets": "earliest"
    }

    # Read data from Kafka
    kafka_source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    base_df = kafka_source.selectExpr("CAST(value as STRING)", "timestamp")
    base_df.printSchema()

    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("temp_condition", StringType(), True),
        StructField("attack", StringType(), True),
        StructField("label", IntegerType(), True)
    ])

    info_dataframe = base_df.select(
        from_json(col("value"), schema).alias("info"), "timestamp"
    )

    info_dataframe.printSchema()
    info_df_fin = info_dataframe.select("info.*", "timestamp")
    info_df_fin.printSchema()

    # Define the output Kafka sink options
    kafka_sink_options = {
        "topic": KAFKA_TOPIC,
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "checkpointLocation": CHECKPOINT_LOCATION
    }

    # Write the processed data to Kafka
    query = (
        info_df_fin.writeStream
        .trigger(processingTime="10 seconds")
        .outputMode("append")
        .format("kafka")
        .options(**kafka_sink_options)
        .start()
    )

    query.awaitTermination()
