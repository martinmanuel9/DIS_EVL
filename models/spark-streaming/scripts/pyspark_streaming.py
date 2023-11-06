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
        
        self.spark.sparkContext.setLogLevel("WARN")
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

    def receive_kafka_message(self):
        sparkDF = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
            .option("subscribePattern", "fridge|garage|gps|light|modbus|thermostat|weather") \
            .option("startingOffsets", "earliest") \
            .load()

        # Filter and process data based on the topic
        filteredDF = sparkDF.filter(sparkDF["topic"].isin("fridge", "garage", "gps", "light", "modbus", "thermostat", "weather"))

        # Process data for each topic separately
        fridgeDF = filteredDF.filter(filteredDF["topic"] == "fridge")
        garageDF = filteredDF.filter(filteredDF["topic"] == "garage")
        gpsDF = filteredDF.filter(filteredDF["topic"] == "gps")
        lightDF = filteredDF.filter(filteredDF["topic"] == "light")
        modbusDF = filteredDF.filter(filteredDF["topic"] == "modbus")
        thermostatDF = filteredDF.filter(filteredDF["topic"] == "thermostat")
        weatherDF = filteredDF.filter(filteredDF["topic"] == "weather")

        # -----------------------------------------------
        # Process data for the "fridge" topic
        # serialFridgeDF = fridgeDF.select("value")
        # fridgeSchema = StructType([
        #     StructField("device", StringType(), True),
        #     StructField("temperature", DoubleType(), True),
        #     StructField("condition", StringType(), True),
        #     StructField("attack", StringType(), True),
        #     StructField("label", IntegerType(), True)])

        # pduUDF = udf(createPdu, fridgeSchema)
        # fridgeDF = serialFridgeDF.select(pduUDF("value").alias("fridgeData"))

        # fridgeReadyDF = fridgeDF.select(
        #     fridgeDF.fridgeData.device,
        #     fridgeDF.fridgeData.temperature,
        #     fridgeDF.fridgeData.condition,
        #     fridgeDF.fridgeData.attack,
        #     fridgeDF.fridgeData.label)

        # uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
        # expandedFridgeDF = fridgeReadyDF.withColumn("uuid", uuid_udf())

        # fridgeQuery = expandedFridgeDF.writeStream \
        #     .outputMode("append") \
        #     .format("console") \
        #     .option("truncate", False) \
        #     .start()
        
        # fridgeQuery.awaitTermination()

        # ----------------------------------------------- 
        # Process data for the "garage" topic
        # serialGarageDF = garageDF.select("value")
        # garageSchema = StructType([
        #     StructField("door_state", StringType(), True),
        #     StructField("sphone", IntegerType(), True), 
        #     StructField("attack", StringType(), True),
        #     StructField("label", IntegerType(), True)])

        # pduUDF = udf(createPdu, garageSchema)
        # garageDF = serialGarageDF.select(pduUDF("value").alias("garageData"))

        # garageReadyDF = garageDF.select( 
        #     garageDF.garageData.door_state,
        #     garageDF.garageData.sphone,
        #     garageDF.garageData.attack,
        #     garageDF.garageData.label)

        # uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
        # expandedGarageDF = garageReadyDF.withColumn("uuid", uuid_udf())

        # garageQuery = expandedGarageDF.writeStream \
        #     .outputMode("append") \
        #     .format("console") \
        #     .option("truncate", False) \
        #     .start()
        
        # garageQuery.awaitTermination()
        
        # -----------------------------------------------
        # Process data for the "gps" topic
        serialGpsDF = gpsDF.select("value")
        
        gpsSchema = StructType([
            StructField("longitude", FloatType(), True),
            StructField("latitude", FloatType(), True), 
            StructField("altitude", FloatType(), True),
            StructField("roll", FloatType(), True),
            StructField("pitch", FloatType(), True),
            StructField("yaw", FloatType(), True),
            StructField("attack", StringType(), True),
            StructField("label", IntegerType(), True)])

        pduUDF = udf(createPdu, gpsSchema)
        gpsDF = serialGpsDF.select(pduUDF("value").alias("gpsData"))

        gpsDF.printSchema()

        gpsReadyDF = gpsDF.select( 
            gpsDF.gpsData.longitude,
            gpsDF.gpsData.latitude,
            gpsDF.gpsData.altitude,
            gpsDF.gpsData.roll,
            gpsDF.gpsData.pitch,
            gpsDF.gpsData.yaw,
            gpsDF.gpsData.attack,
            gpsDF.gpsData.label)

        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
        expandedGpsDF = gpsReadyDF.withColumn("uuid", uuid_udf())

        gpsQuery = expandedGpsDF.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()
        
        gpsQuery.awaitTermination()
        

        # # Define your sink operations
        # cassandra_sink = self.save_to_cassandra(fridgeDF)
        # mysql_sink = self.save_to_mysql(fridgeDF)

if __name__ == "__main__":
    sparkStructuredStreaming = SparkStructuredStreaming()
    sparkStructuredStreaming.receive_kafka_message()
