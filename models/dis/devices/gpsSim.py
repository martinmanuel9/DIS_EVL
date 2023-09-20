#!/usr/bin/env python 

"""
Application:        DIS Simulation of GPS Model 
File name:          gpsSim.py
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
from opendismodel.opendis.dis7 import EntityStatePdu
from opendismodel.opendis.DataOutputStream import DataOutputStream
from opendismodel.opendis.RangeCoordinates import GPS
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import KafkaProducer as kp
import xml.etree.ElementTree as ET

class GPSSim:
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
        gpsDataset = ton.TON_IoT_Datagen(dataset = 'gps')
        self.gpsTrain, self.gpsTest = gpsDataset.create_dataset(train_stepsize=gpsDataset.gpsTrainStepsize, test_stepsize=gpsDataset.gpsTestStepsize, 
                                        train= gpsDataset.completeGPSTrainSet, test = gpsDataset.completeGPSTestSet)

        self.gps = GPS()

    def sendTrainGPS(self):
        columnNames = self.gpsTrain['Dataframe'].columns
        # print(self.gpsTrain['Dataframe'].head())
        for i in range(len(self.gpsTrain['Data'][0])):
            """Sending via PDU and UDP Protocol via Open DIS """
            if self.transmission == 'pdu':
                gpsPDU = EntityStatePdu()
                gpsPDU.entityID.entityID = 42
                gpsPDU.entityID.siteID = 17
                gpsPDU.entityID.applicationID = 23
                gpsPDU.marking.setString('Igor3d')

                gpsLocation = self.gps.llarpy2ecef(np.deg2rad(self.gpsTrain['Data'][0][i][0][3]),   # longitude (radians)   
                                            np.deg2rad(self.gpsTrain['Data'][0][i][0][4]), # latitude (radians)
                                            1,               # altitude (meters)
                                            0,               # roll (radians)
                                            0,               # pitch (radians)
                                            0                # yaw (radians)
                                            )
                
                gpsPDU.entityLocation.x = gpsLocation[0]
                gpsPDU.entityLocation.y = gpsLocation[1]
                gpsPDU.entityLocation.z = gpsLocation[2]
                gpsPDU.entityOrientation.psi = gpsLocation[3]
                gpsPDU.entityOrientation.theta = gpsLocation[4]
                gpsPDU.entityOrientation.phi = gpsLocation[5]

                gpsPDU.attack = self.gpsTrain['Data'][0][i][0][5].encode()
                gpsPDU.label = self.gpsTrain['Data'][0][i][0][6] 

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                gpsPDU.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))
                print('Train Data GPS: \n', 'Longitude: ', self.gpsTrain['Data'][0][i][0][3], '\n', 'Latitude:', self.gpsTrain['Data'][0][i][0][4]) # latitude, longitude
                print('Packet Size: ' , sys.getsizeof(self.gpsTrain['Data'][0][i][0][3]))
                print("Sent {}: {} bytes".format(gpsPDU.__class__.__name__, len(data)))
                time.sleep(10)

            if self.transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("GPSData")
                ET.SubElement(root, "Longitude").text = str(self.gpsTrain['Data'][0][i][0][3])
                ET.SubElement(root, "Latitude").text = str(self.gpsTrain['Data'][0][i][0][4])
                ET.SubElement(root, "Attack").text = str(self.gpsTrain['Data'][0][i][0][5])
                ET.SubElement(root, "Label").text = str(self.gpsTrain['Data'][0][i][0][6])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf8')

                # Send the XML data to Kafka
                self.producer.produce_message(xml_data)

                print('Train Data GPS: \n', 'Longitude: ', self.gpsTrain['Data'][0][i][0][3], '\n', 'Latitude:', self.gpsTrain['Data'][0][i][0][4]) # latitude, longitude
                print('Packet Size: ' , sys.getsizeof(self.gpsTrain['Data'][0][i][0][3]))
                print("Sent {}: {} bytes".format("GPSData", len(xml_data)))
                time.sleep(10)

    def sendTestGPS(self):
        columnNames = self.gpsTest['Dataframe'].columns
        # print(self.gpsTest['Dataframe'].head())
        for i in range(len(self.gpsTest['Data'][0])):
            """Sending via PDU and UDP Protocol via Open DIS """
            if self.transmission == 'pdu':
                gpsPDU = EntityStatePdu()
                gpsPDU.entityID.entityID = 42
                gpsPDU.entityID.siteID = 17
                gpsPDU.entityID.applicationID = 23
                gpsPDU.marking.setString('Igor3d')

                gpsLocation = self.gps.llarpy2ecef(np.deg2rad(self.gpsTest['Data'][0][i][0][3]),   # longitude (radians)   
                                            np.deg2rad(self.gpsTest['Data'][0][i][0][4]), # latitude (radians)
                                            1,               # altitude (meters)
                                            0,               # roll (radians)
                                            0,               # pitch (radians)
                                            0                # yaw (radians)
                                            )
                
                gpsPDU.entityLocation.x = gpsLocation[0]
                gpsPDU.entityLocation.y = gpsLocation[1]
                gpsPDU.entityLocation.z = gpsLocation[2]
                gpsPDU.entityOrientation.psi = gpsLocation[3]
                gpsPDU.entityOrientation.theta = gpsLocation[4]
                gpsPDU.entityOrientation.phi = gpsLocation[5]

                gpsPDU.attack = self.gpsTest['Data'][0][i][0][5].encode()
                gpsPDU.label = self.gpsTest['Data'][0][i][0][6] 

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                gpsPDU.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))
                print('Train Data GPS: \n', 'Longitude: ', self.gpsTest['Data'][0][i][0][3], '\n', 'Latitude:', self.gpsTest['Data'][0][i][0][4]) # latitude, longitude
                print('Packet Size: ' , sys.getsizeof(self.gpsTest['Data'][0][i][0][3]))
                print("Sent {}: {} bytes".format(gpsPDU.__class__.__name__, len(data)))
                time.sleep(10)

            if self.transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("GPSData")
                ET.SubElement(root, "Longitude").text = str(self.gpsTest['Data'][0][i][0][3])
                ET.SubElement(root, "Latitude").text = str(self.gpsTest['Data'][0][i][0][4])
                ET.SubElement(root, "Attack").text = str(self.gpsTest['Data'][0][i][0][5])
                ET.SubElement(root, "Label").text = str(self.gpsTest['Data'][0][i][0][6])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf8')

                # Send the XML data to Kafka
                self.producer.produce_message(xml_data)

                print('Train Data GPS: \n', 'Longitude: ', self.gpsTest['Data'][0][i][0][3], '\n', 'Latitude:', self.gpsTrain['Data'][0][i][0][4]) # latitude, longitude
                print('Packet Size: ' , sys.getsizeof(self.gpsTest['Data'][0][i][0][3]))
                print("Sent {}: {} bytes".format("GPSData", len(xml_data)))
                time.sleep(10)

if __name__ == '__main__':
    gpsSim = GPSSim(transmission='kafka')
    gpsSim.sendTrainGPS()
