#!/usr/bin/env python 

"""
Application:        DIS EVL
File name:          featureExtraction.py
Author:             Martin Manuel Lopez
Creation:           11/22/2023

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

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import *

class FeatureSelection:
   def __init__(self, tableName, dataFrame):
      self.tableName = tableName
      self.dataFrame = dataFrame

   def extractFeature(self):
      if self.tableName == "fridge_table":
         # select the columns that we need for fridge dataset
         # temperature, condition, attack, label
         fridgeExtraction = self.dataFrame.select("temperature", "condition", "attack", "label")
         # need to map condition from low: 0 to high: 1
         fridgeExtraction = fridgeExtraction.withColumn("condition", when(fridgeExtraction.condition == "low", 0).otherwise(1))
         # need to map attack  from normal:0, backdoor: 1, ddos: 2, injection: 3 , password: 4, ransomware: 5, xss: 7
         fridgeExtraction = fridgeExtraction.withColumn("attack", when(fridgeExtraction.attack == "normal", 0).otherwise(when(fridgeExtraction.attack == "backdoor", 1).otherwise(when(fridgeExtraction.attack == "ddos", 2).otherwise(when(fridgeExtraction.attack == "injection", 3).otherwise(when(fridgeExtraction.attack == "password", 4).otherwise(when(fridgeExtraction.attack == "ransomware", 5).otherwise(7)))))))
         # write to console
         # fridgeExtraction.writeStream.format("console").outputMode("append").start()
         return fridgeExtraction

      elif self.tableName == "garage_table":
         # select columns that we need for garage dataset
         # door_state, sphone, attack, label
         garageExtraction = self.dataFrame.select("door_state", "sphone", "attack", "label")
         # need to map door_state from closed: 0 to open: 1
         garageExtraction = garageExtraction.withColumn("door_state", when(garageExtraction.door_state == "closed", 0).otherwise(1))
         # need to map attack from normal:0, backdoor: 1, ddos: 2, injection: 3 , password: 4, ransomware: 5, scanning:6, xss: 7
         garageExtraction = garageExtraction.withColumn("attack", when(garageExtraction.attack == "normal", 0).otherwise(when(garageExtraction.attack == "backdoor", 1).otherwise(when(garageExtraction.attack == "ddos", 2).otherwise(when(garageExtraction.attack == "injection", 3).otherwise(when(garageExtraction.attack == "password", 4).otherwise(when(garageExtraction.attack == "ransomware", 5).otherwise(when(garageExtraction.attack == "scanning", 6).otherwise(7))))))))
         # write to console
         # garageExtraction.writeStream.format("console").outputMode("append").start()
         return garageExtraction
                                               
      elif self.tableName == "gps_table":
         # select columns that we need for gps dataset
         # longitude, latitude, altitude, roll, pitch, yaw, attack, label
         gpsExtraction = self.dataFrame.select("longitude", "latitude", "altitude", "roll", "pitch", "yaw", "attack", "label")
         # need to map out attack from normal:0, backdoor: 1, ddos: 2, injection: 3 , password: 4, ransomware: 5, scanning:6, xss: 7
         gpsExtraction = gpsExtraction.withColumn("attack", when(gpsExtraction.attack == "normal", 0).otherwise(when(gpsExtraction.attack == "backdoor", 1).otherwise(when(gpsExtraction.attack == "ddos", 2).otherwise(when(gpsExtraction.attack == "injection", 3).otherwise(when(gpsExtraction.attack == "password", 4).otherwise(when(gpsExtraction.attack == "ransomware", 5).otherwise(when(gpsExtraction.attack == "scanning", 6).otherwise(7))))))))
         # write to console
         # gpsExtraction.writeStream.format("console").outputMode("append").start()
         return gpsExtraction
      
      elif self.tableName == "light_table":
         # select columns that we need for light dataset
         # motion_status, light_status, attack, label
         lightExtraction = self.dataFrame.select("motion_status", "light_status", "attack", "label")
         # need to map light_status from off: 0 to on: 1
         lightExtraction = lightExtraction.withColumn("light_status", when(lightExtraction.light_status == "off", 0).otherwise(1))
         # need to map attack from normal:0, backdoor: 1, ddos: 2, injection: 3 , password: 4, ransomware: 5, scanning:6, xss: 7
         lightExtraction = lightExtraction.withColumn("attack", when(lightExtraction.attack == "normal", 0).otherwise(when(lightExtraction.attack == "backdoor", 1).otherwise(when(lightExtraction.attack == "ddos", 2).otherwise(when(lightExtraction.attack == "injection", 3).otherwise(when(lightExtraction.attack == "password", 4).otherwise(when(lightExtraction.attack == "ransomware", 5).otherwise(when(lightExtraction.attack == "scanning", 6).otherwise(7))))))))
         # write to console
         # lightExtraction.writeStream.format("console").outputMode("append").start()
         return lightExtraction
      
      elif self.tableName == "modbus_table":
         # select columns that we need for modbus dataset
         # fc1, fc2, fc3, fc4, attack, label
         modbusExtraction = self.dataFrame.select("fc1", "fc2", "fc3", "fc4", "attack", "label")
         # need to map out attack from normal:0, backdoor: 1, ddos: 2, injection: 3 , password: 4, ransomware: 5, scanning:6, xss: 7
         modbusExtraction = modbusExtraction.withColumn("attack", when(modbusExtraction.attack == "normal", 0).otherwise(when(modbusExtraction.attack == "backdoor", 1).otherwise(when(modbusExtraction.attack == "ddos", 2).otherwise(when(modbusExtraction.attack == "injection", 3).otherwise(when(modbusExtraction.attack == "password", 4).otherwise(when(modbusExtraction.attack == "ransomware", 5).otherwise(when(modbusExtraction.attack == "scanning", 6).otherwise(7))))))))
         # write to console
         # modbusExtraction.writeStream.format("console").outputMode("append").start()
         return modbusExtraction
      
      elif self.tableName == "thermostat_table":
         # select columns that we need for thermostat dataset
         # temperature, temp_status, attack, label
         thermostatExtraction = self.dataFrame.select("temperature", "temp_status", "attack", "label")
         # need to map out attack from normal:0, backdoor: 1, ddos: 2, injection: 3 , password: 4, ransomware: 5, scanning:6, xss: 7
         thermostatExtraction = thermostatExtraction.withColumn("attack", when(thermostatExtraction.attack == "normal", 0).otherwise(when(thermostatExtraction.attack == "backdoor", 1).otherwise(when(thermostatExtraction.attack == "ddos", 2).otherwise(when(thermostatExtraction.attack == "injection", 3).otherwise(when(thermostatExtraction.attack == "password", 4).otherwise(when(thermostatExtraction.attack == "ransomware", 5).otherwise(when(thermostatExtraction.attack == "scanning", 6).otherwise(7))))))))
         # write to console
         # thermostatExtraction.writeStream.format("console").outputMode("append").start()
         return thermostatExtraction
      
      elif self.tableName == "weather_table":
         # select columns that we need for weather dataset
         # temperature, pressure, humidity attack, label
         weatherExtraction = self.dataFrame.select("temperature", "pressure", "humidity", "attack", "label")
         # need to map out attack from normal:0, backdoor: 1, ddos: 2, injection: 3 , password: 4, ransomware: 5, scanning:6, xss: 7
         weatherExtraction = weatherExtraction.withColumn("attack", when(weatherExtraction.attack == "normal", 0).otherwise(when(weatherExtraction.attack == "backdoor", 1).otherwise(when(weatherExtraction.attack == "ddos", 2).otherwise(when(weatherExtraction.attack == "injection", 3).otherwise(when(weatherExtraction.attack == "password", 4).otherwise(when(weatherExtraction.attack == "ransomware", 5).otherwise(when(weatherExtraction.attack == "scanning", 6).otherwise(7))))))))
         # write to console
         # weatherExtraction.writeStream.format("console").outputMode("append").start()
         return weatherExtraction
      