from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import uuid
from pyspark.sql.functions import *
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..')))
from opendismodel.opendis.RangeCoordinates import * 
from opendismodel.opendis.PduFactory import createPdu
from opendismodel.opendis.dis7 import *

class SparkStructuredStreaming:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Spark Kafka Structured Streaming Data Pipeline") \
            .config("spark.cassandra.connection.host", "172.18.0.5") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .config("spark.driver.host", "localhost") \
            .getOrCreate()
        
        self.pdu_factory_bc = self.spark.sparkContext.broadcast(createPdu)

    def save_to_cassandra(self, writeDF):
        writeDF.writeStream \
            .outputMode("append") \
            .format("org.apache.spark.sql.cassandra") \
            .option("table", "fridge_table") \
            .option("keyspace", "dis") \
            .start()

    def save_to_mysql(self, writeDF):
        db_credentials = {
            "user": "root",
            "password": "secret",
            "driver": "com.mysql.jdbc.Driver"
        }

        writeDF.writeStream \
            .outputMode("append") \
            .format("jdbc") \
            .option("url", "jdbc:mysql://172.18.0.8:3306/sales_db") \
            .option("dbtable", "fridge") \
            .options(**db_credentials) \
            .start()

    def process_pdu(self, in_pdu):
        pdu = createPdu(in_pdu)
        fridge_data = [
            pdu.device.decode('utf-8'),
            pdu.temperature,
            pdu.condition.decode('utf-8'),
            pdu.attack.decode('utf-8'),
            pdu.label,
            str(uuid.uuid4())
        ]
        return fridge_data

    def process_pdu_message(self, value):
        return udf(self.process_pdu, StructType([
            StructField("device", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("temp_condition", StringType(), True),
            StructField("attack", StringType(), True),
            StructField("label", IntegerType(), True),
            StructField("uuid", StringType(), True)
        ]))(value).alias("fridgeData")

    def receive_kafka_message(self):
        sparkFridgeDF = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
            .option("subscribe", "fridge") \
            .option("startingOffsets", "earliest") \
            .load()

        serialFridgeDF  = sparkFridgeDF.select(["value"])

        fridgeSchema = StructType([ 
            StructField("device", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("temp_condition", StringType(), True),
            StructField("attack", StringType(), True),
            StructField("label", IntegerType(), True),
            StructField("uuid", StringType(), True)])
        
        pduUDF = udf(createPdu, fridgeSchema)

        # need to pass the serialFridgeDF to the process_pdu_message function
        # so that it can be used in the udf
        fridgeDF = serialFridgeDF.select(pduUDF("value").alias("fridgeData"))

        query = fridgeDF.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()

        query.awaitTermination()

        # # Define your sink operations
        # cassandra_sink = self.save_to_cassandra(fridgeDF)
        # mysql_sink = self.save_to_mysql(fridgeDF)

if __name__ == "__main__":
    sparkStructuredStreaming = SparkStructuredStreaming()
    sparkStructuredStreaming.receive_kafka_message()
