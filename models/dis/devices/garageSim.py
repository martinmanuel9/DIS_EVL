#!/usr/bin/env python 

"""
Application:        DIS Simulation of Garage Model 
File name:          garageSim.py
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


class GarageSim:

    def __init__(self):
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Kafka Producer
        self.KAFKA_TOPIC = 'dis'
        self.producer = kp.KafkaProducer('localhost:9092', self.KAFKA_TOPIC)
        

        # Create garage dataset and timesteps for simulation
        garageDataset = ton.TON_IoT_Datagen()
        self.garageTrain, self.garageTest = garageDataset.create_dataset(train_stepsize=garageDataset.garageTrainStepsize, test_stepsize=garageDataset.garageTestStepsize, 
                                        train= garageDataset.completeGarageTrainSet, test = garageDataset.completeGarageTestSet)

    def sendGarageTrain(self, transmission = 'kafka'):
        columnNames = self.garageTrain['Dataframe'].columns
        # print(self.garageTrain['Dataframe'].head())
        for i in range(len(self.garageTrain['Data'][0])):
            if transmission == 'pdu':
                garagePdu = Garage() 
                garagePdu.door_state = self.garageTrain['Data'][0][i][0][3].encode('utf-8')
                garagePdu.sphone = self.garageTrain['Data'][0][i][0][4]
                garagePdu.attack = self.garageTrain['Data'][0][i][0][5].encode('utf-8')
                garagePdu.label = self.garageTrain['Data'][0][i][0][6]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                garagePdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))
                print("Sent {}: {} bytes".format(garagePdu.__class__.__name__, len(data)))
                time.sleep(8)

            """Sending via Kafka Producer"""
            if transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("GarageData")
                ET.SubElement(root, "DoorState").text = str(self.garageTrain['Data'][0][i][0][3])
                ET.SubElement(root, "Sphone").text = str(self.garageTrain['Data'][0][i][0][4])
                ET.SubElement(root, "Attack").text = str(self.garageTrain['Data'][0][i][0][5])
                ET.SubElement(root, "Label").text = str(self.garageTrain['Data'][0][i][0][6])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf8')

                # send xml data to Kafka
                self.producer.produce_message(xml_data)

                print("Sent {}: {} bytes".format("GarageData", len(xml_data)))
                time.sleep(8)

    def sendGarageTest(self, transmission = 'kafka'):
        columnNames = self.garageTest['Dataframe'].columns
        # print(self.garageTest['Dataframe'].head())
        for i in range(len(self.garageTest['Data'][0])):
            if transmission == 'pdu':
                garagePdu = Garage() 
                garagePdu.door_state = self.garageTest['Data'][0][i][0][3].encode('utf-8')
                garagePdu.sphone = self.garageTest['Data'][0][i][0][4]
                garagePdu.attack = self.garageTest['Data'][0][i][0][5].encode('utf-8')
                garagePdu.label = self.garageTest['Data'][0][i][0][6]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                garagePdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))
                print("Sent {}: {} bytes".format(garagePdu.__class__.__name__, len(data)))
                time.sleep(8)

            """Sending via Kafka Producer"""
            if transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("GarageData")
                ET.SubElement(root, "DoorState").text = str(self.garageTest['Data'][0][i][0][3])
                ET.SubElement(root, "Sphone").text = str(self.garageTest['Data'][0][i][0][4])
                ET.SubElement(root, "Attack").text = str(self.garageTest['Data'][0][i][0][5])
                ET.SubElement(root, "Label").text = str(self.garageTest['Data'][0][i][0][6])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf8')

                # send xml data to Kafka
                self.producer.produce_message(xml_data)

                print("Sent {}: {} bytes".format("GarageData", len(xml_data)))
                time.sleep(8)

if __name__ == "__main__":
    GarageSim = GarageSim()
    GarageSim.sendGarageTrain(transmission = 'kafka')