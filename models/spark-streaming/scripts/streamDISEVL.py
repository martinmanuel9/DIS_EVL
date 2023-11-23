#!/usr/bin/env python 

"""
Application:        PySpark Pipeline for Streaming DIS and utilizing EVL
File name:          streamDISEVL.py 
Author:             Martin Manuel Lopez
Creation:           10/05/2023

The University of Arizona
Department of Electrical and Computer Engineering
College of Engineering
"""

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
from pyspark.sql.types import *
import os
import uuid
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..')))
from opendismodel.opendis.RangeCoordinates import * 
from opendismodel.opendis.PduFactory import createPdu
from opendismodel.opendis.dis7 import *
from saveCassandra import CassandraSink
from saveMySQL import MySQLSink
from featureExtraction import FeatureSelection

class SparkStructuredStreaming:
    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("DISEVL_Pipeline") \
            .config("spark.cassandra.connection.host", "172.18.0.5") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .config("spark.driver.host", "localhost") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR") # WARN
        self.pdu_factory_bc = self.spark.sparkContext.broadcast(createPdu)
        
    def receive_kafka_message(self):
        sparkDF = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
            .option("subscribePattern", "fridge|garage|gps|light|modbus|thermostat|weather") \
            .option("failOnDataLoss", "false") \
            .option("startingOffsets", "earliest") \
            .load()
        
        # # Filter and process data based on the topic
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
        # ----------------------------------------------- 
        serialFridgeDF = fridgeDF.select("value")
        fridgeSchema = StructType([
            StructField("device", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("condition", StringType(), True),
            StructField("attack", StringType(), True),
            StructField("label", IntegerType(), True)])

        fridgeUDF = udf(createPdu, fridgeSchema)
        fridgeDF = serialFridgeDF.select(fridgeUDF("value").alias("fridgeData"))

        fridgeReadyDF = fridgeDF.select(
            fridgeDF.fridgeData.device.alias("device"),
            fridgeDF.fridgeData.temperature.alias("temperature"),
            fridgeDF.fridgeData.condition.alias("condition"),
            fridgeDF.fridgeData.attack.alias("attack"),
            fridgeDF.fridgeData.label.alias("label"))

        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
        expandedFridgeDF = fridgeReadyDF.withColumn("uuid", uuid_udf())

        # Save to Cassandra
        cassandraSink = CassandraSink(keyspace="dis", table="fridge_table")
        cassandraSink.write(expandedFridgeDF)

        # Save to MySQL
        mySqlSink = MySQLSink(table="fridge_table")
        mySqlSink.write(expandedFridgeDF)

        # feature extraction for Fridge 
        fridgeFeatures = FeatureSelection(tableName="fridge_table", dataFrame=expandedFridgeDF).extractFeature()
        

        # ----------------------------------------------- 
        # Process data for the "garage" topic
        # -----------------------------------------------
        serialGarageDF = garageDF.select("value")
        garageSchema = StructType([
            StructField("door_state", StringType(), True),
            StructField("sphone", IntegerType(), True), 
            StructField("attack", StringType(), True),
            StructField("label", IntegerType(), True)])

        garageUDF = udf(createPdu, garageSchema)
        garageDF = serialGarageDF.select(garageUDF("value").alias("garageData"))

        garageReadyDF = garageDF.select( 
            garageDF.garageData.door_state.alias("door_state"),
            garageDF.garageData.sphone.alias("sphone"),
            garageDF.garageData.attack.alias("attack"),
            garageDF.garageData.label.alias("label"))

        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
        expandedGarageDF = garageReadyDF.withColumn("uuid", uuid_udf())
        
        # Save to Cassandra
        cassandraSink = CassandraSink(keyspace="dis", table="garage_table")
        cassandraSink.write(expandedGarageDF)

        # Save to MySQL
        mySqlSink = MySQLSink(table="garage_table")
        mySqlSink.write(expandedGarageDF)

        # feature extraction for Garage
        garageFeatures = FeatureSelection(tableName="garage_table", dataFrame=expandedGarageDF).extractFeature()

        # -----------------------------------------------
        # Process data for the "gps" topic 
        # -----------------------------------------------
        serialGpsDF = gpsDF.select("value")
        gpsSchema = StructType([
            StructField("entityLocation", StructType([
                StructField("x", FloatType(), True),
                StructField("y", FloatType(), True),
                StructField("z", FloatType(), True)]), True),
            StructField("entityOrientation", StructType([
                StructField("psi", FloatType(), True),
                StructField("theta", FloatType(), True),
                StructField("phi", FloatType(), True)]), True),
            StructField("attack", StringType(), True),
            StructField("label", IntegerType(), True)    
            ])

        gpsUDF = udf(createPdu, gpsSchema)
        gpsDF = serialGpsDF.select(gpsUDF("value").alias("gpsData")) 

        gpsReadyDF = gpsDF.select( 
            gpsDF.gpsData.entityLocation.x.alias("longitude"),
            gpsDF.gpsData.entityLocation.y.alias("latitude"),
            gpsDF.gpsData.entityLocation.z.alias("altitude"),
            gpsDF.gpsData.entityOrientation.psi.alias("roll"),
            gpsDF.gpsData.entityOrientation.theta.alias("pitch"),
            gpsDF.gpsData.entityOrientation.phi.alias("yaw"),
            gpsDF.gpsData.attack.alias("attack"),
            gpsDF.gpsData.label.alias("label"))

        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
        expandedGpsDF = gpsReadyDF.withColumn("uuid", uuid_udf())
        
        # Save to Cassandra
        cassandraSink = CassandraSink(keyspace="dis", table="gps_table")
        cassandraSink.write(expandedGpsDF)

        # Save to MySQL
        mySqlSink = MySQLSink(table="gps_table")
        mySqlSink.write(expandedGpsDF)

        # feature extraction for GPS
        gpsFeatures = FeatureSelection(tableName="gps_table", dataFrame=expandedGpsDF).extractFeature()
            
        # -----------------------------------------------
        # Process data for the "light" topic  
        # -----------------------------------------------
        serialLightDF = lightDF.select("value")
        lightSchema = StructType([
            StructField("motion_status", StringType(), True),
            StructField("light_status", StringType(), True),
            StructField("attack", StringType(), True),
            StructField("label", IntegerType(), True)])
        
        lightUDF = udf(createPdu, lightSchema)
        lightDF = serialLightDF.select(lightUDF("value").alias("lightData"))

        lightReadyDF = lightDF.select(
            lightDF.lightData.motion_status.alias("motion_status"),
            lightDF.lightData.light_status.alias("light_status"),
            lightDF.lightData.attack.alias("attack"),
            lightDF.lightData.label.alias("label"))
        
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
        expandedLightDF = lightReadyDF.withColumn("uuid", uuid_udf())

        # Save to Cassandra
        cassandraSink = CassandraSink(keyspace="dis", table="light_table")
        cassandraSink.write(expandedLightDF)

        # Save to MySQL
        mySqlSink = MySQLSink(table="light_table")
        mySqlSink.write(expandedLightDF)

        # feature extraction for Light
        lightFeatures = FeatureSelection(tableName="light_table", dataFrame=expandedLightDF).extractFeature()

        # -----------------------------------------------
        # Process data for the "modbus" topic  
        # -----------------------------------------------
        serialModbusDF = modbusDF.select("value")
        modbusSchema = StructType([
            StructField("fc1", DoubleType(), True),
            StructField("fc2", DoubleType(), True),
            StructField("fc3", DoubleType(), True),
            StructField("fc4", DoubleType(), True),
            StructField("attack", StringType(), True),
            StructField("label", IntegerType(), True)])
        
        modbusUDF = udf(createPdu, modbusSchema)
        modbusDF = serialModbusDF.select(modbusUDF("value").alias("modbusData"))

        modbusReadyDF = modbusDF.select(
            modbusDF.modbusData.fc1.alias("fc1"),
            modbusDF.modbusData.fc2.alias("fc2"),
            modbusDF.modbusData.fc3.alias("fc3"),
            modbusDF.modbusData.fc4.alias("fc4"),
            modbusDF.modbusData.attack.alias("attack"),
            modbusDF.modbusData.label.alias("label"))
        
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
        expandedModbusDF = modbusReadyDF.withColumn("uuid", uuid_udf())
        
        # Save to Cassandra
        cassandraSink = CassandraSink(keyspace="dis", table="modbus_table")
        cassandraSink.write(expandedModbusDF)

        # Save to MySQL
        mySqlSink = MySQLSink(table="modbus_table")
        mySqlSink.write(expandedModbusDF)

        # feature extraction for Modbus
        modbusFeatures = FeatureSelection(tableName="modbus_table", dataFrame=expandedModbusDF).extractFeature()

        # -----------------------------------------------
        # Process data for the "thermostat" topic
        # ----------------------------------------------- 
        serialThermostatDF = thermostatDF.select("value")
        thermostatSchema = StructType([
            StructField("device", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("temp_status", IntegerType(), True),
            StructField("attack", StringType(), True),
            StructField("label", IntegerType(), True)])
        
        thermostatUDF = udf(createPdu, thermostatSchema)
        thermostatDF = serialThermostatDF.select(thermostatUDF("value").alias("thermostatData"))

        thermostatReadyDF = thermostatDF.select(
            thermostatDF.thermostatData.device.alias("device"),
            thermostatDF.thermostatData.temperature.alias("temperature"),
            thermostatDF.thermostatData.temp_status.alias("temp_status"),
            thermostatDF.thermostatData.attack.alias("attack"),
            thermostatDF.thermostatData.label.alias("label"))
        
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
        expandedThermostatDF = thermostatReadyDF.withColumn("uuid", uuid_udf())

        # Save to Cassandra 
        cassandraSink = CassandraSink(keyspace="dis", table="thermostat_table")
        cassandraSink.write(expandedThermostatDF)

        # Save to MySQL
        mySqlSink = MySQLSink(table="thermostat_table")
        mySqlSink.write(expandedThermostatDF)

        # feature extraction for Thermostat
        thermostatFeatures = FeatureSelection(tableName="thermostat_table", dataFrame=expandedThermostatDF).extractFeature()
            
        # -----------------------------------------------
        # Process data for the "weather" topic 
        # -----------------------------------------------
        serialWeatherDF = weatherDF.select("value")
        weatherSchema = StructType([
            StructField("device", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("attack", StringType(), True),
            StructField("label", IntegerType(), True)])
        
        weatherUDF = udf(createPdu, weatherSchema)
        weatherDF = serialWeatherDF.select(weatherUDF("value").alias("weatherData"))

        weatherReadyDF = weatherDF.select(
            weatherDF.weatherData.device.alias("device"),
            weatherDF.weatherData.temperature.alias("temperature"),
            weatherDF.weatherData.pressure.alias("pressure"),
            weatherDF.weatherData.humidity.alias("humidity"),
            weatherDF.weatherData.attack.alias("attack"),
            weatherDF.weatherData.label.alias("label"))
        
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
        expandedWeatherDF = weatherReadyDF.withColumn("uuid", uuid_udf())
        
        # Save to Cassandra
        cassandraSink = CassandraSink(keyspace="dis", table="weather_table")
        cassandraSink.write(expandedWeatherDF)

        # Save to MySQL
        mySqlSink = MySQLSink(table="weather_table")
        mySqlSink.write(expandedWeatherDF)

        # feature extraction for Weather
        weatherFeatures = FeatureSelection(tableName="weather_table", dataFrame=expandedWeatherDF).extractFeature()
            
        # ----------------------------------------------

if __name__ == "__main__":
    sparkStructuredStreaming = SparkStructuredStreaming()
    while True:
        sparkStructuredStreaming.receive_kafka_message()
        sparkStructuredStreaming.spark.streams.awaitAnyTermination()
