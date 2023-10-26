from pyspark.sql import SparkSession
from pyspark.sql.types import *
import uuid
import os
from confluent_kafka import Consumer, KafkaError
import logging
import json
from pyspark.sql.functions import *
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..')))
from opendismodel.opendis.RangeCoordinates import * 
from opendismodel.opendis.PduFactory import createPdu
from opendismodel.opendis.dis7 import *

BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "dis"

def save_to_cassandra(writeDF, epoch_id):
    print("Printing epoch_id: ")
    print(epoch_id)

    writeDF.write \
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="fridge_table", keyspace="dis")\
        .save()

    print(epoch_id, "saved to Cassandra")

def save_to_mysql(writeDF, epoch_id):
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

    print(epoch_id, "saved to MySQL")


def kafa_read_dis_stream(topic, group_id, transmission):
    
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.auto.offset.store': False,
        'enable.partition.eof': False
    })

    consumer.subscribe([topic])

    # set up schema here 
    # Define the schema for the Kafka message
    fridgeSchema = StructType([ 
        StructField("device", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("temp_condition", StringType(), True),
        StructField("attack", StringType(), True),
        StructField("label", StringType(), True),
        StructField("uuid", StringType(), True)
    ])

    # sparkFridgeDF = spark.createDataFrame([], fridgeSchema)
    # # Create a Kafka DataFrame for streaming
    # sparkFridgeDF = spark.readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    #     .option("subscribe", "fridge") \
    #     .option("startingOffsets", "earliest") \
    #     .load()
    
    # expandedFridgeDF = sparkFridgeDF \
    #     .selectExpr("CAST(value AS STRING)") \
    #     .select(from_json(col("value"), fridgeSchema, options={'mode': 'PERMISSIVE'}).alias("fridge")) \
    #     .select("fridge.*")
            
    # # Generate a UUID column
    # uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
    # expandedFridgeDF = expandedFridgeDF.withColumn("uuid", uuid_udf())

    # # Start the streaming query
    # fridgeQuery = expandedFridgeDF.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start() \
    #     .awaitTermination()
    

    garageSchema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("door_state", StringType(), True),
        StructField("sphone", IntegerType(), True),
        StructField("attack", StringType(), True),
        StructField("label", IntegerType(), True)
    ])
    sparkGarageDF = spark.createDataFrame([], garageSchema)

    gpsSchema = StructType([
        StructField("timestamp", TimestampType(), True), 
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("altitude", DoubleType(), True),
        StructField("roll", DoubleType(), True),
        StructField("pitch", DoubleType(), True),
        StructField("yaw", DoubleType(), True),
        StructField("attack", StringType(), True),
        StructField("label", IntegerType(), True)
    ])
    sparkGPSDF = spark.createDataFrame([], gpsSchema)

    lightSchema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("motion_status", IntegerType(), True),
        StructField("light_status", StringType(), True),
        StructField("attack", StringType(), True),
        StructField("label", IntegerType(), True)
    ])
    sparkLightDF = spark.createDataFrame([], lightSchema)

    modbusSchema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("fc1", DoubleType(), True),
        StructField("fc2", DoubleType(), True),
        StructField("fc3", DoubleType(), True),
        StructField("fc4", DoubleType(), True),
        StructField("attack", StringType(), True),
        StructField("label", IntegerType(), True)
    ])
    sparkModbusDF = spark.createDataFrame([], modbusSchema)

    thermostatSchema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("temp_status", IntegerType(), True),
        StructField("attack", StringType(), True),
        StructField("label", IntegerType(), True)
    ])
    sparkThermostatDF = spark.createDataFrame([], thermostatSchema)

    weatherSchema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("attack", StringType(), True),
        StructField("label", IntegerType(), True)
    ])
    sparkWeatherDF = spark.createDataFrame([], weatherSchema)

    # Create a Spark DataFrame for streaming
    spark_df = spark.createDataFrame([], StringType())

 
    while True:
        msg = consumer.poll(timeout=60.0) 
        
        if msg is None:
            print("Exiting due to timeout. No messages received within 60 seconds.")
            break
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

                            # create a spark dataframe
                            timestamp = msg.timestamp()[1] // 1000.0
                            # convert timestamp to datetime
                            date_time = datetime.fromtimestamp(timestamp)
                        
                            new_row = Row(
                                timestamp= date_time,
                                latitude= body[0],
                                longitude= body[1],
                                altitude= body[2],
                                yaw= body[3],
                                pitch= body[4],
                                roll= body[5],
                                attack= pdu.attack.decode('utf-8'),
                                label= pdu.label)
                            
                            # Append the new Row to the DataFrame
                            appended_df = sparkGPSDF.union(spark.createDataFrame([new_row], gpsSchema))
 

                        elif pdu.pduType == 73:  # Light

                            sparkLightDF.append((
                                msg.timestamp()[1] // 1000.0,
                                pdu.motion_status,
                                pdu.light_state.decode('utf-8'),
                                pdu.attack.decode('utf-8'),
                                pdu.label
                            ))
                        

                        elif pdu.pduType == 70:  # environment
                            if pdu.device.decode('utf-8') == 'Fridge':
                                # Define a UDF to apply the createPdu function to the Kafka messages
                                createPduUDF = udf(createPdu, StringType())

                                sparkFridgeDF = spark.readStream \
                                    .format("kafka") \
                                    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
                                    .option("subscribe", "fridge") \
                                    .option("startingOffsets", "earliest") \
                                    .load()
                                 

                                # # Assign the `value` column to another variable
                                # messageDF = sparkFridgeDF.select("value")
                                # fridgePDU = createPdu(messageDF.first()[0])

                                # # creates uuid
                                # uuid_udf = str(uuid.uuid4())
                                # # Create a Row
                                # new_row = Row(
                                #     device=fridgePDU.device.decode('utf-8'),
                                #     temperature=fridgePDU.temperature,
                                #     temp_condition=fridgePDU.condition.decode('utf-8'),
                                #     attack=fridgePDU.attack.decode('utf-8'),
                                #     label=fridgePDU.label,
                                #     uuid=uuid_udf)
                                
                                # # Create a DataFrame using the schema and the data
                                # fridge_row_DF = spark.createDataFrame([new_row], fridgeSchema)

                                messageDF = sparkFridgeDF.select(createPduUDF(sparkFridgeDF["value"]).alias("fridgePDU"))
                                # messageDF.show()

                                # # Generate a UUID column
                                uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
                                expandedFridgeDF = messageDF.withColumn("uuid", uuid_udf())
                                
                                # expandedFridgeDF.show()
                                

                                # Define a query to print the first message
                                query = expandedFridgeDF.writeStream \
                                    .outputMode("append") \
                                    .format("console") \
                                    .option("truncate", False) \
                                    .start()
                                
                                # Start the streaming query
                                query.awaitTermination()

                                          

                            if pdu.device.decode('utf-8') == 'Thermostat':
                                timestamp = msg.timestamp()[1] // 1000.0
                                # convert timestamp to datetime
                                date_time = datetime.fromtimestamp(timestamp)

                                new_row = Row(
                                    timestamp= date_time,
                                    temperature=pdu.temperature,
                                    temp_status=pdu.temp_status,
                                    attack=pdu.attack.decode('utf-8'),
                                    label=pdu.label)
                                
                                # Append the new Row to the DataFrame
                                appended_df = sparkThermostatDF.union(spark.createDataFrame([new_row], thermostatSchema))

                                # Show the result
                                appended_df.show()
                            
                            if pdu.device.decode('utf-8') == 'Weather':
                                timestamp = msg.timestamp()[1] // 1000.0
                                # convert timestamp to datetime
                                date_time = datetime.fromtimestamp(timestamp)

                                new_row = Row(
                                    timestamp= date_time,
                                    temperature=pdu.temperature,
                                    pressure=pdu.pressure,
                                    humidity=pdu.humidity,
                                    attack=pdu.attack.decode('utf-8'),
                                    label=pdu.label)
                                
                                # Append the new Row to the DataFrame
                                appended_df = sparkWeatherDF.union(spark.createDataFrame([new_row], weatherSchema))

                                # Show the result
                                appended_df.show()
                            

                        elif pdu.pduType == 71:  # modbus
                            timestamp = msg.timestamp()[1] // 1000.0
                            # convert timestamp to datetime
                            date_time = datetime.fromtimestamp(timestamp)

                            new_row = Row(
                                timestamp= date_time,
                                fc1=pdu.fc1,
                                fc2=pdu.fc2,
                                fc3=pdu.fc3,
                                fc4=pdu.fc4,
                                attack=pdu.attack.decode('utf-8'),
                                label=pdu.label)
                            
                            # Append the new Row to the DataFrame
                            appended_df = sparkModbusDF.union(spark.createDataFrame([new_row], modbusSchema))

                            # Show the result
                            appended_df.show()
                    


                        elif pdu.pduType == 72:  # garage
                            timestamp = msg.timestamp()[1] // 1000.0
                            # convert timestamp to datetime
                            date_time = datetime.fromtimestamp(timestamp)

                            new_row = Row(
                                timestamp= date_time,
                                door_state=pdu.door_state.decode('utf-8'),
                                sphone=pdu.sphone,
                                attack=pdu.attack.decode('utf-8'),
                                label=pdu.label)
                            
                            # Append the new Row to the DataFrame
                            appended_df = sparkGarageDF.union(spark.createDataFrame([new_row], garageSchema))

                            # Show the result
                            appended_df.show()

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
                    consumer.commit(msg)

                except UnicodeDecodeError as e:
                    print("UnicodeDecodeError: ", e)
                except Exception as e:
                    logging.error(f"Error processing message: {str(e)}")
            else:
                logging.error("Received message is not a byte-like object.")
            

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark Kafka Streaming Data Pipeline") \
    .config("spark.cassandra.connection.host", "172.18.0.5") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# # Go through the PDU kafka messages
kafa_read_dis_stream(topic='fridge', group_id=GROUP_ID, 
                   transmission='kafka_pdu')



# # Create a Kafka DataFrame for streaming
# input_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
#     .option("subscribe", "fridge") \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Extract and parse the message value
# expanded_df = input_df \
#     .selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), fridgeSchema).alias("fridge")) \
#     .select("fridge.*")

# # Generate a UUID column
# uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
# expanded_df = expanded_df.withColumn("uuid", uuid_udf())

# # Output to Console
# console_query = expanded_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# # Define a checkpoint location for the streaming query
# checkpoint_location = os.getcwd()

# # Output to Cassandra
# cassandra_query = expanded_df.writeStream \
#     .trigger(processingTime="15 seconds") \
#     .outputMode("append") \
#     .option("checkpointLocation", checkpoint_location) \
#     .foreachBatch(save_to_cassandra) \
#     .start()

# # Output to MySQL
# mysql_query = expanded_df.writeStream \
#     .trigger(processingTime="15 seconds") \
#     .outputMode("append") \
#     .option("checkpointLocation", checkpoint_location) \
#     .foreachBatch(save_to_mysql) \
#     .start()

# # Wait for the streaming queries to terminate
# console_query.awaitTermination()
# cassandra_query.awaitTermination()
# mysql_query.awaitTermination()
