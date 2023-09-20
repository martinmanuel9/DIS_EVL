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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from evl import ton_iot_dis_datagen as ton
from opendismodel.opendis.dis7 import *
from opendismodel.opendis.DataOutputStream import DataOutputStream
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import KafkaProducer as kp
import xml.etree.ElementTree as ET

class FridgeSim:
    def __init__(self, transmission):
        self.transmission = transmission
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self.transmission == 'kafka':
            # Kafka Producer
            self.KAFKA_TOPIC = 'dis'
            self.producer = kp.KafkaProducer('localhost:9092', self.KAFKA_TOPIC)
        

        # Create garage dataset and timesteps for simulation
        fridgeDataset = ton.TON_IoT_Datagen(dataset='fridge')
        self.fridgeTrain, self.fridgeTest = fridgeDataset.create_dataset(train_stepsize=fridgeDataset.fridgeTrainStepsize, test_stepsize=fridgeDataset.fridgeTestStepsize, 
                                        train= fridgeDataset.completeFridgeTrainSet, test = fridgeDataset.completeFridgeTestSet)


    def sendFridgeTrain(self ):
        columnNames = self.fridgeTrain['Dataframe'].columns
        # print(self.fridgeTrain['Dataframe'].head())
        for i in range(len(self.fridgeTrain['Data'][0])):
            """Sending via PDU and UDP Protocol via Open DIS """
            if self.transmission == 'pdu':
                fridgeEnvPdu = Environment()
                fridgeEnvPdu.temperature = self.fridgeTrain['Data'][0][i][0][3] # fridge row  
                fridgeEnvPdu.condition = self.fridgeTrain['Data'][0][i][0][4].encode('utf-8')
                fridgeEnvPdu.attack = self.fridgeTrain['Data'][0][i][0][5].encode('utf-8') # attack
                fridgeEnvPdu.label = int(self.fridgeTrain['Data'][0][i][0][6])  #label

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                fridgeEnvPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print('Fridge Temp Row: ', self.fridgeTrain['Data'][0][i][0][3])
                print('Fridge Temp Condition: ' , self.fridgeTrain['Data'][0][i][0][4])
                print('Fridge Temp Attack: ', self.fridgeTrain['Data'][0][i][0][5])
                print('Fridge Label: ', self.fridgeTrain['Data'][0][i][0][6])
                print("Sent {}: {} bytes".format(fridgeEnvPdu.__class__.__name__, len(data)))
                time.sleep(5)

            """Sending via Kafka Producer"""
            if self.transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("FridgeData")
                ET.SubElement(root, "FridgeTempRow").text = str(self.fridgeTrain['Data'][0][i][0][3])
                ET.SubElement(root, "FridgeTempCondition").text = str(self.fridgeTrain['Data'][0][i][0][4])
                ET.SubElement(root, "Attack").text = str(self.fridgeTrain['Data'][0][i][0][5])
                ET.SubElement(root, "Label").text = str(self.fridgeTrain['Data'][0][i][0][6])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf-8')

                # Send the XML data to Kafka
                self.producer.produce_message(xml_data)

                print('Fridge Temp Row: ', self.fridgeTrain['Data'][0][i][0][3])
                print('Fridge Temp Condition: ' , self.fridgeTrain['Data'][0][i][0][4])
                print('Fridge Temp Attack: ', self.fridgeTrain['Data'][0][i][0][5])
                print('Fridge Label: ', self.fridgeTrain['Data'][0][i][0][6])
                print("Sent {}: {} bytes".format("FridgeData", len(xml_data)))
                time.sleep(5)

    def sendFridgeTest(self):
        columnNames = self.fridgeTest['Dataframe'].columns
        # print(self.fridgeTest['Dataframe'].head())
        for i in range(len(self.fridgeTest['Data'][0])):
            """Sending via PDU and UDP Protocol via Open DIS """
            if self.transmission == 'pdu':
                fridgeEnvPdu = Environment()
                fridgeEnvPdu.temperature = self.fridgeTest['Data'][0][i][0][3] # fridge row  
                fridgeEnvPdu.condition = self.fridgeTest['Data'][0][i][0][4].encode('utf-8')
                fridgeEnvPdu.attack = self.fridgeTest['Data'][0][i][0][5].encode('utf-8') # attack
                fridgeEnvPdu.label = int(self.fridgeTest['Data'][0][i][0][6])  #label

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                fridgeEnvPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print('Fridge Temp Row: ', self.fridgeTest['Data'][0][i][0][3])
                print('Fridge Temp Condition: ' , self.fridgeTest['Data'][0][i][0][4])
                print('Fridge Temp Attack: ', self.fridgeTest['Data'][0][i][0][5])
                print('Fridge Label: ', self.fridgeTest['Data'][0][i][0][6])
                print("Sent {}: {} bytes".format(fridgeEnvPdu.__class__.__name__, len(data)))
                time.sleep(5)

            """Sending via Kafka Producer"""
            if self.transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("FridgeData")
                ET.SubElement(root, "FridgeTempRow").text = str(self.fridgeTest['Data'][0][i][0][3])
                ET.SubElement(root, "FridgeTempCondition").text = str(self.fridgeTest['Data'][0][i][0][4])
                ET.SubElement(root, "Attack").text = str(self.fridgeTest['Data'][0][i][0][5])
                ET.SubElement(root, "Label").text = str(self.fridgeTest['Data'][0][i][0][6])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf-8')

                # Send the XML data to Kafka
                self.producer.produce_message(xml_data)

                print('Fridge Temp Row: ', self.fridgeTest['Data'][0][i][0][3])
                print('Fridge Temp Condition: ' , self.fridgeTest['Data'][0][i][0][4])
                print('Fridge Temp Attack: ', self.fridgeTest['Data'][0][i][0][5])
                print('Fridge Label: ', self.fridgeTest['Data'][0][i][0][6])
                print("Sent {}: {} bytes".format("FridgeData", len(xml_data)))
                time.sleep(5)


if __name__ == "__main__":
    FridgeSim = FridgeSim(transmission='pdu')
    FridgeSim.sendFridgeTrain()
