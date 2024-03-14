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


class GarageSim:

    def __init__(self, transmission, speed):
        self.transmission = transmission
        self.speed = speed
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self.transmission == 'kafka' or self.transmission == 'kafka_pdu':
            # Kafka Producer
            self.KAFKA_TOPIC = 'garage'
            self.producer = kp.KafkaProducer('172.18.0.4:9092', self.KAFKA_TOPIC)
        

        # Create garage dataset and timesteps for simulation
        garageDataset = ton.TON_IoT_Datagen(dataset= 'garage')
        self.garageTrain, self.garageTest = garageDataset.create_dataset(train_stepsize=garageDataset.garageTrainStepsize, test_stepsize=garageDataset.garageTestStepsize, 
                                        train= garageDataset.completeGarageTrainSet, test = garageDataset.completeGarageTestSet)
        
        # print(self.garageTrain['Dataframe'].head())

    def sendGarageTrain(self):
        columnNames = self.garageTrain['Dataframe'].columns
        # print(self.garageTrain['Dataframe'].head())
        for i in range(len(self.garageTrain['Dataframe'])):
            if self.transmission == 'pdu':
                garagePdu = Garage() 
                garagePdu.door_state = self.garageTrain['Dataframe']['door_state'][i].encode('utf-8')
                garagePdu.sphone = self.garageTrain['Dataframe']['sphone_signal'][i]
                garagePdu.attack = self.garageTrain['Dataframe']['type'][i].encode('utf-8')
                garagePdu.label = self.garageTrain['Dataframe']['label'][i]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                garagePdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(garagePdu.__class__.__name__, len(data)) 
                    + "\n Garage Data Sent:"
                    + "\n  Door State     : {}".format(garagePdu.door_state.decode('utf-8'))
                    + "\n  Sphone         : {}".format(garagePdu.sphone)
                    + "\n  Attack         : {}".format(garagePdu.attack.decode('utf-8'))
                    + "\n  Label          : {}\n".format(garagePdu.label)
                    )
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))

            """Sending via Kafka Producer"""
            if self.transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("GarageData")
                ET.SubElement(root, "DoorState").text = str(self.garageTrain['Dataframe']['door_state'][i])
                ET.SubElement(root, "Sphone").text = str(self.garageTrain['Dataframe']['sphone_signal'][i])
                ET.SubElement(root, "Attack").text = str(self.garageTrain['Dataframe']['type'][i])
                ET.SubElement(root, "Label").text = str(self.garageTrain['Dataframe']['label'][i])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf8')

                # send xml data to Kafka
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("GarageData", len(xml_data)) 
                    + "\n Garage Data Sent:"
                    + "\n  Door State     : {}".format(self.garageTrain['Dataframe']['door_state'][i])
                    + "\n  Sphone         : {}".format(self.garageTrain['Dataframe']['sphone_signal'][i])
                    + "\n  Attack         : {}".format(self.garageTrain['Dataframe']['type'][i])
                    + "\n  Label          : {}\n".format(self.garageTrain['Dataframe']['label'][i])
                    )
                
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))

            if self.transmission == 'kafka_pdu':
                garagePdu = Garage() 
                garagePdu.door_state = self.garageTrain['Dataframe']['door_state'][i].encode('utf-8')
                garagePdu.sphone = self.garageTrain['Dataframe']['sphone_signal'][i]
                garagePdu.attack = self.garageTrain['Dataframe']['type'][i].encode('utf-8')
                garagePdu.label = self.garageTrain['Dataframe']['label'][i]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                garagePdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(garagePdu.__class__.__name__, len(data)) 
                    + "\n Garage Data Sent:"
                    + "\n  Door State     : {}".format(garagePdu.door_state.decode('utf-8'))
                    + "\n  Sphone         : {}".format(garagePdu.sphone)
                    + "\n  Attack         : {}".format(garagePdu.attack.decode('utf-8'))
                    + "\n  Label          : {}\n".format(garagePdu.label)
                    )
                
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))


    def sendGarageTest(self):
        columnNames = self.garageTest['Dataframe'].columns
        # print(self.garageTest['Dataframe'].head())
        for i in range(len(self.garageTest['Dataframe'])):
            if self.transmission == 'pdu':
                garagePdu = Garage() 
                garagePdu.door_state = self.garageTest['Dataframe']['door_state'][i].encode('utf-8')
                garagePdu.sphone = self.garageTest['Dataframe']['sphone_signal'][i]
                garagePdu.attack = self.garageTest['Dataframe']['type'][i].encode('utf-8')
                garagePdu.label = self.garageTest['Dataframe']['label'][i]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                garagePdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(garagePdu.__class__.__name__, len(data)) 
                    + "\n Garage Data Sent:"
                    + "\n  Door State     : {}".format(garagePdu.door_state.decode('utf-8'))
                    + "\n  Sphone         : {}".format(garagePdu.sphone)
                    + "\n  Attack         : {}".format(garagePdu.attack.decode('utf-8'))
                    + "\n  Label          : {}\n".format(garagePdu.label)
                    )
                
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))

            """Sending via Kafka Producer"""
            if self.transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("GarageData")
                ET.SubElement(root, "DoorState").text = str(self.garageTest['Dataframe']['door_state'][i])
                ET.SubElement(root, "Sphone").text = str(self.garageTest['Dataframe']['sphone_signal'][i])
                ET.SubElement(root, "Attack").text = str(self.garageTest['Dataframe']['type'][i])
                ET.SubElement(root, "Label").text = str(self.garageTest['Dataframe']['label'][i])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf8')

                # send xml data to Kafka
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("GarageData", len(xml_data))
                    + "\n Garage Data Sent:"
                    + "\n  Door State     : {}".format(self.garageTest['Dataframe']['door_state'][i])
                    + "\n  Sphone         : {}".format(self.garageTest['Dataframe']['sphone_signal'][i])
                    + "\n  Attack         : {}".format(self.garageTest['Dataframe']['type'][i])
                    + "\n  Label          : {}\n".format(self.garageTest['Dataframe']['label'][i])
                    )
                
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))
            
            if self.transmission == 'kafka_pdu':
                garagePdu = Garage() 
                garagePdu.door_state = self.garageTest['Dataframe']['door_state'][i].encode('utf-8')
                garagePdu.sphone = self.garageTest['Dataframe']['sphone_signal'][i]
                garagePdu.attack = self.garageTest['Dataframe']['type'][i].encode('utf-8')
                garagePdu.label = self.garageTest['Dataframe']['label'][i]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                garagePdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(garagePdu.__class__.__name__, len(data)) 
                    + "\n Garage Data Sent:"
                    + "\n  Door State     : {}".format(garagePdu.door_state.decode('utf-8'))
                    + "\n  Sphone         : {}".format(garagePdu.sphone)
                    + "\n  Attack         : {}".format(garagePdu.attack.decode('utf-8'))
                    + "\n  Label          : {}\n".format(garagePdu.label)
                    )
                
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))


# if __name__ == "__main__":
#     GarageSim = GarageSim(transmission='kafka_pdu')
#     GarageSim.sendGarageTrain()