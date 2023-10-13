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

from io import BytesIO
from opendismodel.opendis.RangeCoordinates import *
from opendismodel.opendis.PduFactory import createPdu
from opendismodel.opendis.dis7 import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, StringType, IntegerType
import uuid
from confluent_kafka import Consumer, KafkaError
import time
import sys
import os
import logging


BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "dis"


def save_to_cassandra(self, writeDF, epoch_id):
    print("Printing epoch_id: ")
    print(epoch_id)

    writeDF.write \
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="fridge_table", keyspace="dis")\
        .save()

    print(epoch_id, "saved to Cassandra")


def save_to_mysql(self, writeDF, epoch_id):
    db_credentials = {
        "user": "root",
        "password": "secret",
        "driver": "com.mysql.jdbc.Driver"
    }

    print("Printing epoch_id: ")
    print(epoch_id)

    writeDF.write \
        .jdbc(
            url="jdbc:mysql://172.18.0.8:3306/sales_db",
            table="fridge",
            mode="append",
            properties=db_credentials
        )

    print(epoch_id, "saved to mysql")


def kafka_pdu_consumer(self, topic, group_id, transmission, spark_df):
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Disable auto-commit of offsets
        'enable.auto.offset.store': False,  # Disable automatic offset storage
        'enable.partition.eof': False  # Disable automatic partition EOF event
    })

    consumer.subscribe([topic])
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
        else:
            message = msg.value()
            if isinstance(message, bytes):
                try:
                    # ------- Sending PDUs via Kafka -------#
                    if transmission == 'kafka_pdu':
                        pdu = createPdu(message)
                        pduTypeName = pdu.__class__.__name__

                        if pdu.pduType == 1:  # PduTypeDecoders.EntityStatePdu:
                            gps = GPS()
                            loc = (pdu.entityLocation.x,
                                   pdu.entityLocation.y,
                                   pdu.entityLocation.z,
                                   pdu.entityOrientation.psi,
                                   pdu.entityOrientation.theta,
                                   pdu.entityOrientation.phi)
                            gps.update(loc)
                            body = gps.ecef2llarpy(*loc)
                            spark_df.append((
                                pdu.entityID.entityID,
                                body[0],
                                body[1],
                                body[2],
                                body[3],
                                body[4],
                                body[5],
                                pdu.attack.decode('utf-8'),
                                pdu.label
                            ))
                            print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                  + " Id          : {}\n".format(pdu.entityID.entityID)
                                    + " Latitude    : {:.2f} degrees\n".format(rad2deg(body[0]))
                                    + " Longitude   : {:.2f} degrees\n".format(rad2deg(body[1]))
                                    + " Altitude    : {:.0f} meters\n".format(body[2])
                                    + " Yaw         : {:.2f} degrees\n".format(rad2deg(body[3]))
                                    + " Pitch       : {:.2f} degrees\n".format(rad2deg(body[4]))
                                    + " Roll        : {:.2f} degrees\n".format(rad2deg(body[5]))
                                    + " Attack      : {}\n".format(pdu.attack.decode('utf-8'))
                                    + " Label       : {}\n".format(pdu.label))

                        elif pdu.pduType == 73:  # Light
                            spark_df.append((
                                pdu.motion_status,
                                pdu.light_state.decode('utf-8'),
                                pdu.attack.decode('utf-8'),
                                pdu.label
                            ))
                            print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                  + " Motion Status : {}\n".format(pdu.motion_status)
                                    + " Light Status  : {}\n".format(pdu.light_status.decode('utf-8'))
                                    + " Attack        : {}\n".format(pdu.attack.decode('utf-8'))
                                    + " Label         : {}\n".format(pdu.label))

                        elif pdu.pduType == 70:  # environment
                            spark_df.append((
                                pdu.device.decode('utf-8'),
                                pdu.temperature,
                                pdu.pressure,
                                pdu.humidity,
                                pdu.condition.decode('utf-8'),
                                pdu.temp_status,
                                pdu.attack.decode('utf-8'),
                                pdu.label
                            ))
                            print("Received {}: {} Bytes \n".format(pduTypeName, len(message), flush=True)
                                  + " Device      : {}\n".format(pdu.device.decode('utf-8'))
                                    + " Temperature : {}\n".format(pdu.temperature)
                                    + " Pressure    : {}\n".format(pdu.pressure)
                                    + " Humidity    : {}\n".format(pdu.humidity)
                                    + " Condition   : {}\n".format(pdu.condition.decode('utf-8'))
                                    + " Temp Status : {}\n".format(pdu.temp_status)
                                    + " Attack      : {}\n".format(pdu.attack.decode('utf-8'))
                                    + " Label       : {}\n".format(pdu.label))

                        elif pdu.pduType == 71:  # modbus
                            spark_df.append((
                                pdu.fc1,
                                pdu.fc2,
                                pdu.fc3,
                                pdu.fc4,
                                pdu.attack.decode('utf-8'),
                                pdu.label
                            ))
                            print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                  + " FC1 Register    : {}\n".format(pdu.fc1)
                                    + " FC2 Discrete    : {}\n".format(pdu.fc2)
                                    + " FC3 Register    : {}\n".format(pdu.fc3)
                                    + " FC4 Read Coil   : {}\n".format(pdu.fc4)
                                    + " Attack          : {}\n".format(pdu.attack.decode('utf-8'))
                                    + " Label           : {}\n".format(pdu.label))

                        elif pdu.pduType == 72:  # garage
                            spark_df.append((
                                pdu.door_state.decode('utf-8'),
                                pdu.sphone,
                                pdu.attack.decode('utf-8'),
                                pdu.label
                            ))
                            print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                  + " Door State: {}\n".format(pdu.door_state.decode('utf-8'))
                                    + " SPhone: {}\n".format(pdu.sphone)
                                    + " Attack: {}\n".format(pdu.attack.decode('utf-8'))
                                    + " Label : {}\n".format(pdu.label))

                        else:
                            print("Received PDU {}, {} bytes".format(
                                pduTypeName, len(message)), flush=True)

                    # ------ Regular Kafka Messages ------#
                    else:
                        message = message.decode('utf-8')
                        spark_df.append((
                            message.decode('utf-8'),
                        ))
                        logging.info(f"Received message: {message}")

                    # --- Commit the offset manually --- #
                    self.consumer.commit(msg)

                except UnicodeDecodeError as e:
                    print("UnicodeDecodeError: ", e)
            else:
                logging.error("Received message is not a byte-like object.")


fridgeSchema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("temp_condition", StringType(), True),
    StructField("attack", StringType(), True),
    StructField("label", IntegerType(), True)
])

spark = SparkSession \
    .builder \
    .appName("Spark Kafka Streaming Data Pipeline") \
    .master("local[*]") \
    .config("spark.cassandra.connection.host", "172.18.0.5") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.driver.host", "localhost")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

fridge_pdu_data = []
while True:
    kafka_pdu_consumer(topic="fridge", group_id="dis",
                       transmission="kafka_pdu")

input_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "fridge") \
    .option("startingOffsets", "earliest") \
    .load()

input_df.printSchema()

expanded_df = input_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), fridgeSchema).alias("order")) \
    .select("order.*")

uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
expanded_df = expanded_df.withColumn("uuid", uuid_udf())
expanded_df.printSchema()

# Output to Console
expanded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()

query1 = expanded_df.writeStream \
    .trigger(processingTime="15 seconds") \
    .foreachBatch(save_to_cassandra) \
    .outputMode("update") \
    .start()

query2 = expanded_df.writeStream \
    .trigger(processingTime="15 seconds") \
    .outputMode("complete") \
    .foreachBatch(save_to_mysql) \
    .start()

# query2.awaitTermination()
