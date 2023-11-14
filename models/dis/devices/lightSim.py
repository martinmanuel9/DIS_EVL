#!/usr/bin/env python 

"""
Application:        DIS Simulation of Light Model 
File name:          lightSim.py
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

class LightSim:

    def __init__(self, transmission):
        self.transmission = transmission
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self.transmission == 'kafka' or self.transmission == 'kafka_pdu':
            # Kafka Producer
            self.KAFKA_TOPIC = 'light'
            self.producer = kp.KafkaProducer('172.18.0.4:9092', self.KAFKA_TOPIC)
        

        # Create garage dataset and timesteps for simulation
        lightDataset = ton.TON_IoT_Datagen(dataset = 'light')
        self.lightTrain, self.lightTest = lightDataset.create_dataset(train_stepsize=lightDataset.lightTrainStepsize, test_stepsize=lightDataset.lightTestStepsize, 
                                        train= lightDataset.completeLightTrainSet, test = lightDataset.completeLightTestSet)

    def sendLightTrain(self):
        columnNames = self.lightTrain['Dataframe'].columns
        # print(self.lightTrain['Dataframe'].head())
        for i in range(len(self.lightTrain['Data'][0])):
            if self.transmission == 'pdu':
                lightTrainPdu = Light()
                lightTrainPdu.motion_status = self.lightTrain['Data'][0][i][0][3] # motion status
                lightTrainPdu.light_status = self.lightTrain['Data'][0][i][0][4].encode() #light status
                lightTrainPdu.attack = self.lightTrain['Data'][0][i][0][5].encode()
                lightTrainPdu.label = self.lightTrain['Data'][0][i][0][6]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                lightTrainPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(lightTrainPdu.__class__.__name__, len(data)) 
                    + "\n Light Data Sent:"
                    + "\n  Motion Status : {}".format(lightTrainPdu.motion_status)
                    + "\n  Light Status  : {}".format(lightTrainPdu.light_status.decode('utf-8'))
                    + "\n  Attack        : {}".format(lightTrainPdu.attack.decode('utf-8'))
                    + "\n  Label         : {}\n".format(lightTrainPdu.label)
                    )

                time.sleep(random.uniform(0, 2))
            
            if self.transmission == 'kafka':
                # Create an XML element for each row in the dataframe
                root = ET.Element('LightData')
                ET.SubElement(root, 'MotionStatus').text = str(self.lightTrain['Data'][0][i][0][3])
                ET.SubElement(root, 'LightStatus').text = str(self.lightTrain['Data'][0][i][0][4])
                ET.SubElement(root, 'Attack').text = str(self.lightTrain['Data'][0][i][0][5])
                ET.SubElement(root, 'Label').text = str(self.lightTrain['Data'][0][i][0][6])

                # Create XML string
                xml_data = ET.tostring(root, encoding='utf8')

                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("LightData", len(xml_data))
                    + "\n Light Data Sent:"
                    + "\n  Motion Status : {}".format(self.lightTrain['Data'][0][i][0][3])
                    + "\n  Light Status  : {}".format(self.lightTrain['Data'][0][i][0][4])
                    + "\n  Attack        : {}".format(self.lightTrain['Data'][0][i][0][5])
                    + "\n  Label         : {}\n".format(self.lightTrain['Data'][0][i][0][6])
                    )
                
                time.sleep(random.uniform(0, 2))

            if self.transmission == 'kafka_pdu':
                lightTrainPdu = Light()
                lightTrainPdu.motion_status = self.lightTrain['Data'][0][i][0][3] # motion status
                lightTrainPdu.light_status = self.lightTrain['Data'][0][i][0][4].encode() #light status
                lightTrainPdu.attack = self.lightTrain['Data'][0][i][0][5].encode()
                lightTrainPdu.label = self.lightTrain['Data'][0][i][0][6]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                lightTrainPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(lightTrainPdu.__class__.__name__, len(data)) 
                    + "\n Light Data Sent:"
                    + "\n  Motion Status : {}".format(lightTrainPdu.motion_status)
                    + "\n  Light Status  : {}".format(lightTrainPdu.light_status.decode('utf-8'))
                    + "\n  Attack        : {}".format(lightTrainPdu.attack.decode('utf-8'))
                    + "\n  Label         : {}\n".format(lightTrainPdu.label)
                    )

                time.sleep(random.uniform(0, 2))

    def sendLightTest(self):
        columnNames = self.lightTest['Dataframe'].columns
        # print(self.lightTest['Dataframe'].head())
        for i in range(len(self.lightTrain['Data'][0])):
            if self.transmission == 'pdu':
                lightPdu = Light()
                lightPdu.motion_status = self.lightTest['Data'][0][i][0][3] # motion status
                lightPdu.light_status = self.lightTest['Data'][0][i][0][4].encode() #light status
                lightPdu.attack = self.lightTest['Data'][0][i][0][5].encode()
                lightPdu.label = self.lightTest['Data'][0][i][0][6]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                lightPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(lightPdu.__class__.__name__, len(data)) 
                    + "\n Light Data Sent:"
                    + "\n  Motion Status : {}".format(lightPdu.motion_status)
                    + "\n  Light Status  : {}".format(lightPdu.light_status.decode('utf-8'))
                    + "\n  Attack        : {}".format(lightPdu.attack.decode('utf-8'))
                    + "\n  Label         : {}\n".format(lightPdu.label)
                    )
                time.sleep(random.uniform(0, 2))
            
            if self.transmission == 'kafka':
                # Create an XML element for each row in the dataframe
                root = ET.Element('LightData')
                ET.SubElement(root, 'MotionStatus').text = str(self.lightTest['Data'][0][i][0][3])
                ET.SubElement(root, 'LightStatus').text = str(self.lightTest['Data'][0][i][0][4])
                ET.SubElement(root, 'Attack').text = str(self.lightTest['Data'][0][i][0][5])
                ET.SubElement(root, 'Label').text = str(self.lightTest['Data'][0][i][0][6])

                # Create XML string
                xml_data = ET.tostring(root, encoding='utf8')

                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("LightData", len(xml_data))
                    + "\n Light Data Sent:"
                    + "\n  Motion Status : {}".format(self.lightTest['Data'][0][i][0][3])
                    + "\n  Light Status  : {}".format(self.lightTest['Data'][0][i][0][4])
                    + "\n  Attack        : {}".format(self.lightTest['Data'][0][i][0][5])
                    + "\n  Label         : {}\n".format(self.lightTest['Data'][0][i][0][6])
                    )
                
                time.sleep(random.uniform(0, 2))

            if self.transmission == 'kafka_pdu':
                lightPdu = Light()
                lightPdu.motion_status = self.lightTest['Data'][0][i][0][3] # motion status
                lightPdu.light_status = self.lightTest['Data'][0][i][0][4].encode() #light status
                lightPdu.attack = self.lightTest['Data'][0][i][0][5].encode()
                lightPdu.label = self.lightTest['Data'][0][i][0][6]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                lightPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data) 

                print("Sent {} PDU: {} bytes".format(lightPdu.__class__.__name__, len(data)) 
                    + "\n Light Data Sent:"
                    + "\n  Motion Status : {}".format(lightPdu.motion_status)
                    + "\n  Light Status  : {}".format(lightPdu.light_status.decode('utf-8'))
                    + "\n  Attack        : {}".format(lightPdu.attack.decode('utf-8'))
                    + "\n  Label         : {}\n".format(lightPdu.label)
                    )
                
                time.sleep(random.uniform(0, 2))


if __name__ == '__main__':
    LightSim = LightSim(transmission= 'kafka_pdu')
    LightSim.sendLightTrain()
