#!/usr/bin/env python 

"""
Application:        DIS Simulation of Weather Model 
File name:          weatherSim.py
Author:             Martin Manuel Lopez
Creation:           8/28/2023

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

import os
import sys
import socket 
import time 
from io import BytesIO
import numpy as np
import pandas as pd
import random
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from evl import ton_iot_dis_datagen as ton
from opendismodel.opendis.dis7 import *
from opendismodel.opendis.DataOutputStream import DataOutputStream
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import KafkaProducer as kp
import xml.etree.ElementTree as ET

class WeatherSim:
    def __init__(self, transmission):
        self.transmission = transmission
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self.transmission == 'kafka' or self.transmission == 'kafka_pdu':
            # Kafka Producer
            self.KAFKA_TOPIC = 'weather'
            self.producer = kp.KafkaProducer('172.18.0.4:9092', self.KAFKA_TOPIC)


        # Create garage dataset and timesteps for simulation
        weatherDataset = ton.TON_IoT_Datagen(dataset = 'weather')
        self.weatherTrain, self.weatherTest = weatherDataset.create_dataset(train_stepsize=weatherDataset.weatherTrainStepsize, test_stepsize=weatherDataset.weatherTestStepsize, 
                                        train= weatherDataset.completeWeatherTrainSet, test = weatherDataset.completeWeatherTestSet)

    def sendWeatherTrain(self):
        columnNames = self.weatherTrain['Dataframe'].columns
        # print(self.weatherTrain['Dataframe'].head())
        # print( self.weatherTrain['Dataframe'][self.weatherTrain['Dataframe']['label'].isnull()])
        for i in range((len(self.weatherTrain['Data'][0]))):
            if self.transmission == 'pdu':
                weatherPdu = Environment()
                device = "Weather"
                weatherPdu.device = device.encode('utf-8') # device
                weatherPdu.temperature = self.weatherTrain['Data'][0][i][0][3] # tempeature
                weatherPdu.pressure = self.weatherTrain['Data'][0][i][0][4] # pressure
                weatherPdu.humidity = self.weatherTrain['Data'][0][i][0][5] # humidity 
                weatherPdu.attack = self.weatherTrain['Data'][0][i][0][6].encode('utf-8')
                weatherPdu.label = self.weatherTrain['Data'][0][i][0][7] 

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                weatherPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                
                print("Sent {} PDU: {} bytes".format(weatherPdu.__class__.__name__, len(data)) 
                    + "\n Weather Data Sent:"
                    + "\n  Device          : {}".format(weatherPdu.device.decode('utf-8'))
                    + "\n  Temperature     : {}".format(weatherPdu.temperature)
                    + "\n  Pressure        : {}".format(weatherPdu.pressure)
                    + "\n  Humidity        : {}".format(weatherPdu.humidity)
                    + "\n  Attack          : {}".format(weatherPdu.attack.decode('utf-8'))
                    + "\n  Label           : {}\n".format(weatherPdu.label)
                    )

                time.sleep(random.uniform(0, 5))

            elif self.transmission == 'kafka':
                # Create an XML element for each row in the dataframe
                root = ET.Element('WeatherData')
                ET.SubElement(root, 'Temperature').text = str(self.weatherTrain['Data'][0][i][0][3])
                ET.SubElement(root, 'Pressure').text = str(self.weatherTrain['Data'][0][i][0][4])
                ET.SubElement(root, 'Humidity').text = str(self.weatherTrain['Data'][0][i][0][5])
                ET.SubElement(root, 'Attack').text = str(self.weatherTrain['Data'][0][i][0][6])
                ET.SubElement(root, 'Label').text = str(self.weatherTrain['Data'][0][i][0][7])

                # Convert XML element to string
                xml_data = ET.tostring(root, encoding='utf8', method='xml')

                # Send string to Kafka Producer
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("WeatherData", len(xml_data))
                    + "\n Weather Data Sent:"
                    + "\n  Temperature     : {}".format(self.weatherTrain['Data'][0][i][0][3])
                    + "\n  Pressure        : {}".format(self.weatherTrain['Data'][0][i][0][4])
                    + "\n  Humidity        : {}".format(self.weatherTrain['Data'][0][i][0][5])
                    + "\n  Attack          : {}".format(self.weatherTrain['Data'][0][i][0][6])
                    + "\n  Label           : {}".format(self.weatherTrain['Data'][0][i][0][7])
                    )
                  
                time.sleep(random.uniform(0, 5))

            elif self.transmission == 'kafka_pdu':
                weatherPdu = Environment()
                device = "Weather"
                weatherPdu.device = device.encode('utf-8') # device
                weatherPdu.temperature = self.weatherTrain['Data'][0][i][0][3] # tempeature
                weatherPdu.pressure = self.weatherTrain['Data'][0][i][0][4] # pressure
                weatherPdu.humidity = self.weatherTrain['Data'][0][i][0][5] # humidity 
                weatherPdu.attack = self.weatherTrain['Data'][0][i][0][6].encode('utf-8')
                weatherPdu.label = self.weatherTrain['Data'][0][i][0][7] 

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                weatherPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)
                
                print("Sent {} PDU: {} bytes".format(weatherPdu.__class__.__name__, len(data)) 
                    + "\n Weather Data Sent:"
                    + "\n  Device          : {}".format(weatherPdu.device.decode('utf-8'))
                    + "\n  Temperature     : {}".format(weatherPdu.temperature)
                    + "\n  Pressure        : {}".format(weatherPdu.pressure)
                    + "\n  Humidity        : {}".format(weatherPdu.humidity)
                    + "\n  Attack          : {}".format(weatherPdu.attack.decode('utf-8'))
                    + "\n  Label           : {}\n".format(weatherPdu.label)
                    )

                time.sleep(random.uniform(0, 5))

    def sendWeatherTest(self ):
        columnNames = self.weatherTest['Dataframe'].columns
        # print(self.weatherTrain['Dataframe'].head())
        for i in range((len(self.weatherTrain['Data'][0]))):
            if self.transmission == 'pdu':
                weatherPdu = Environment()
                device = "Weather"
                weatherPdu.device = device.encode('utf-8') # device
                weatherPdu.temperature = self.weatherTest['Data'][0][i][0][3] # tempeature
                weatherPdu.pressure = self.weatherTest['Data'][0][i][0][4] # pressure
                weatherPdu.humidity = self.weatherTest['Data'][0][i][0][5] # humidity 
                weatherPdu.attack = self.weatherTest['Data'][0][i][0][6].encode('utf-8')
                weatherPdu.label = self.weatherTest['Data'][0][i][0][7]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                weatherPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(weatherPdu.__class__.__name__, len(data)) 
                    + "\n Weather Data Sent:"
                    + "\n  Device          : {}".format(weatherPdu.device.decode('utf-8'))
                    + "\n  Temperature     : {}".format(weatherPdu.temperature)
                    + "\n  Pressure        : {}".format(weatherPdu.pressure)
                    + "\n  Humidity        : {}".format(weatherPdu.humidity)
                    + "\n  Attack          : {}".format(weatherPdu.attack.decode('utf-8'))
                    + "\n  Label           : {}\n".format(weatherPdu.label)
                    )
                
                time.sleep(random.uniform(0, 5))

            if self.transmission == 'kafka':
                # Create an XML element for each row in the dataframe
                root = ET.Element('WeatherData')
                ET.SubElement(root, 'Temperature').text = str(self.weatherTest['Data'][0][i][0][3])
                ET.SubElement(root, 'Pressure').text = str(self.weatherTest['Data'][0][i][0][4])
                ET.SubElement(root, 'Humidity').text = str(self.weatherTest['Data'][0][i][0][5])
                ET.SubElement(root, 'Attack').text = str(self.weatherTest['Data'][0][i][0][6])
                ET.SubElement(root, 'Label').text = str(self.weatherTest['Data'][0][i][0][7])

                # Convert XML element to string
                xml_data = ET.tostring(root, encoding='utf8', method='xml')

                # Send string to Kafka Producer
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("WeatherData", len(xml_data))
                    + "\n Weather Data Sent:"
                    + "\n  Temperature     : {}".format(self.weatherTrain['Data'][0][i][0][3])
                    + "\n  Pressure        : {}".format(self.weatherTrain['Data'][0][i][0][4])
                    + "\n  Humidity        : {}".format(self.weatherTrain['Data'][0][i][0][5])
                    + "\n  Attack          : {}".format(self.weatherTrain['Data'][0][i][0][6])
                    + "\n  Label           : {}".format(self.weatherTrain['Data'][0][i][0][7])
                    )
                time.sleep(random.uniform(0, 5))

            if self.transmission == 'kafka_pdu':
                weatherPdu = Environment()
                device = "Weather"
                weatherPdu.device = device.encode('utf-8') # device
                weatherPdu.temperature = self.weatherTest['Data'][0][i][0][3] # tempeature
                weatherPdu.pressure = self.weatherTest['Data'][0][i][0][4] # pressure
                weatherPdu.humidity = self.weatherTest['Data'][0][i][0][5] # humidity 
                weatherPdu.attack = self.weatherTest['Data'][0][i][0][6].encode('utf-8')
                weatherPdu.label = self.weatherTest['Data'][0][i][0][7]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                weatherPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(weatherPdu.__class__.__name__, len(data)) 
                    + "\n Weather Data Sent:"
                    + "\n  Device          : {}".format(weatherPdu.device.decode('utf-8'))
                    + "\n  Temperature     : {}".format(weatherPdu.temperature)
                    + "\n  Pressure        : {}".format(weatherPdu.pressure)
                    + "\n  Humidity        : {}".format(weatherPdu.humidity)
                    + "\n  Attack          : {}".format(weatherPdu.attack.decode('utf-8'))
                    + "\n  Label           : {}\n".format(weatherPdu.label)
                    )
                
                time.sleep(random.uniform(0, 5))


# if __name__ == '__main__':
#     weatherSim = WeatherSim(transmission= 'kafka_pdu')
#     weatherSim.sendWeatherTrain()
