#!/usr/bin/env python 

"""
Application:        DIS Simulation of Fridge Model 
File name:          fridgeSim.py
Author:             Martin Manuel Lopez
Creation:           8/28/2023

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

import os
import sys
import socket 
import time 
from io import BytesIO
import numpy as np
import pandas as pd
import random
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from opendismodel.opendis.dis7 import *
from opendismodel.opendis.DataOutputStream import DataOutputStream
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import ton_iot_dis_datagen as ton
import KafkaProducer as kp
import xml.etree.ElementTree as ET


class FridgeSim:
    def __init__(self, transmission, speed):
        self.transmission = transmission
        self.speed = speed
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self.transmission == 'kafka' or self.transmission == 'kafka_pdu':
            # Kafka Producer
            self.KAFKA_TOPIC = 'fridge'
            self.producer = kp.KafkaProducer('172.18.0.4:9092', self.KAFKA_TOPIC)
        

        # Create garage dataset and timesteps for simulation
        fridgeDataset = ton.TON_IoT_Datagen(dataset='fridge')
        self.fridgeTrain, self.fridgeTest = fridgeDataset.create_dataset(train_stepsize=fridgeDataset.fridgeTrainStepsize, test_stepsize=fridgeDataset.fridgeTestStepsize, 
                                        train= fridgeDataset.completeFridgeTrainSet, test = fridgeDataset.completeFridgeTestSet)
        

    def sendFridgeTrain(self ):
        columnNames = self.fridgeTrain['Dataframe'].columns
        # print(self.fridgeTrain['Dataframe'].head())
        for i in range(len(self.fridgeTrain['Dataframe'])):
            """Sending via PDU and UDP Protocol via Open DIS """
            if self.transmission == 'pdu':
                fridgeEnvPdu = Environment()
                device = "Fridge"
                fridgeEnvPdu.device = device.encode('utf-8')
                fridgeEnvPdu.temperature = self.fridgeTrain['Dataframe']['fridge_temperature'][i] # fridge 
                fridgeEnvPdu.condition = self.fridgeTrain['Dataframe']['temp_condition'][i].encode('utf-8')
                fridgeEnvPdu.attack = self.fridgeTrain['Dataframe']['type'][i].encode('utf-8') # attack
                fridgeEnvPdu.label = int(self.fridgeTrain['Dataframe']['label'][i])  #label

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                fridgeEnvPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(fridgeEnvPdu.__class__.__name__, len(data))
                    + "\n Fridge Data Sent:"
                    + "\n  Device       : {}".format(fridgeEnvPdu.device.decode('utf-8'))
                    + "\n  Temperature  : {}".format(fridgeEnvPdu.temperature)
                    + "\n  Condition    : {}".format(fridgeEnvPdu.condition.decode('utf-8'))
                    + "\n  Attack       : {}".format(fridgeEnvPdu.attack.decode('utf-8'))
                    + "\n  Label        : {}\n".format(fridgeEnvPdu.label)
                )
                
                if self.speed == 'slow':
                    time.sleep(2)

            """Sending via Kafka Producer"""
            if self.transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("FridgeData")
                ET.SubElement(root, "FridgeTempRow").text = str(self.fridgeTrain['Dataframe']['fridge_temperature'][i])
                ET.SubElement(root, "FridgeTempCondition").text = str(self.fridgeTrain['Dataframe']['temp_condition'][i])
                ET.SubElement(root, "Attack").text = str(self.fridgeTrain['Dataframe']['type'][i])
                ET.SubElement(root, "Label").text = str(self.fridgeTrain['Dataframe']['label'][i])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf-8')

                # Send the XML data to Kafka
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("FridgeData", len(xml_data))
                    + "\n Fridge Data Sent:"
                    + "\n Temperature     : {}".format(self.fridgeTrain['Dataframe']['fridge_temperature'][i])
                    + "\n Temp Condition  : {}".format(self.fridgeTrain['Dataframe']['temp_condition'][i])
                    + "\n Attack          : {}".format(self.fridgeTrain['Dataframe']['type'][i])
                    + "\n Label           : {}\n".format(self.fridgeTrain['Dataframe']['label'][i])
                    )
                
                if self.speed == 'slow':
                    time.sleep(2)
            
            if self.transmission == 'kafka_pdu':
                # send pdu via kafka
                fridgeEnvPdu = Environment()
                device = "Fridge"
                fridgeEnvPdu.device = device.encode('utf-8')
                fridgeEnvPdu.temperature = self.fridgeTrain['Dataframe']['fridge_temperature'][i] # fridge 
                fridgeEnvPdu.condition = self.fridgeTrain['Dataframe']['temp_condition'][i].encode('utf-8')
                fridgeEnvPdu.attack = self.fridgeTrain['Dataframe']['type'][i].encode('utf-8') # attack
                fridgeEnvPdu.label = int(self.fridgeTrain['Dataframe']['label'][i])  #label

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                fridgeEnvPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(fridgeEnvPdu.__class__.__name__, len(data))
                    + "\n Fridge Data Sent:"
                    + "\n  Device       : {}".format(fridgeEnvPdu.device.decode('utf-8'))
                    + "\n  Temperature  : {}".format(fridgeEnvPdu.temperature)
                    + "\n  Condition    : {}".format(fridgeEnvPdu.condition.decode('utf-8'))
                    + "\n  Attack       : {}".format(fridgeEnvPdu.attack.decode('utf-8'))
                    + "\n  Label        : {}\n".format(fridgeEnvPdu.label)
                )
                
                if self.speed == 'slow':
                    time.sleep(2)

    def sendFridgeTest(self):
        columnNames = self.fridgeTest['Dataframe'].columns
        # print(self.fridgeTest['Dataframe'].head())
        for i in range(len(self.fridgeTest['Dataframe'])):
            """Sending via PDU and UDP Protocol via Open DIS """
            if self.transmission == 'pdu':
                fridgeEnvPdu = Environment()
                device = "Fridge"
                fridgeEnvPdu.device = device.encode('utf-8')
                fridgeEnvPdu.temperature = self.fridgeTest['Dataframe']['fridge_temperature'][i] # fridge row  
                fridgeEnvPdu.condition = self.fridgeTest['Dataframe']['temp_condition'][i].encode('utf-8')
                fridgeEnvPdu.attack = self.fridgeTest['Dataframe']['type'][i].encode('utf-8') # attack
                fridgeEnvPdu.label = int(self.fridgeTest['Dataframe']['label'][i])  #label

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                fridgeEnvPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(fridgeEnvPdu.__class__.__name__, len(data))
                    + "\n Fridge Data Sent:"
                    + "\n  Device       : {}".format(fridgeEnvPdu.device.decode('utf-8'))
                    + "\n  Temperature  : {}".format(fridgeEnvPdu.temperature)
                    + "\n  Condition    : {}".format(fridgeEnvPdu.condition.decode('utf-8'))
                    + "\n  Attack       : {}".format(fridgeEnvPdu.attack.decode('utf-8'))
                    + "\n  Label        : {}\n".format(fridgeEnvPdu.label)
                )
                if self.speed == 'slow':
                    time.sleep(2)

            """Sending via Kafka Producer"""
            if self.transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("FridgeData")
                ET.SubElement(root, "FridgeTempRow").text = str(self.fridgeTest['Dataframe']['fridge_temperature'][i])
                ET.SubElement(root, "FridgeTempCondition").text = str(self.fridgeTest['Dataframe']['temp_condition'][i])
                ET.SubElement(root, "Attack").text = str(self.fridgeTest['Dataframe']['type'][i])
                ET.SubElement(root, "Label").text = str(self.fridgeTest['Dataframe']['label'][i])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf-8')

                # Send the XML data to Kafka
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("FridgeData", len(xml_data))
                    + "\n Fridge Data Sent:"
                    + "\n Temperature     : {}".format(self.fridgeTest['Dataframe']['fridge_temperature'][i])
                    + "\n Temp Condition  : {}".format(self.fridgeTest['Dataframe']['temp_condition'][i])
                    + "\n Attack          : {}".format(self.fridgeTest['Dataframe']['type'][i])
                    + "\n Label           : {}\n".format(self.fridgeTest['Dataframe']['label'][i])
                    )
                if self.speed == 'slow':
                    time.sleep(2)

            if self.transmission == 'kafka_pdu':
                # send pdu via kafka
                fridgeEnvPdu = Environment()
                device = "Fridge"
                fridgeEnvPdu.device = device.encode('utf-8')
                fridgeEnvPdu.temperature = self.fridgeTest['Dataframe']['fridge_temperature'][i] # fridge 
                fridgeEnvPdu.condition = self.fridgeTest['Dataframe']['temp_condition'][i].encode('utf-8')
                fridgeEnvPdu.attack = self.fridgeTest['Dataframe']['type'][i].encode('utf-8') # attack
                fridgeEnvPdu.label = int(self.fridgeTest['Dataframe']['label'][i])  #label

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                fridgeEnvPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(fridgeEnvPdu.__class__.__name__, len(data))
                    + "\n Fridge Data Sent:"
                    + "\n  Device       : {}".format(fridgeEnvPdu.device.decode('utf-8'))
                    + "\n  Temperature  : {}".format(fridgeEnvPdu.temperature)
                    + "\n  Condition    : {}".format(fridgeEnvPdu.condition.decode('utf-8'))
                    + "\n  Attack       : {}".format(fridgeEnvPdu.attack.decode('utf-8'))
                    + "\n  Label        : {}\n".format(fridgeEnvPdu.label)
                )
                if self.speed == 'slow':
                    time.sleep(2)
                


# if __name__ == "__main__":
#     FridgeSim = FridgeSim(transmission= 'kafka_pdu')
#     FridgeSim.sendFridgeTrain()
