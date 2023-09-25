#!/usr/bin/env python 

"""
Application:        DIS Simulation of Thermostat Model 
File name:          thermostatSim.py
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

class ThermostatSim:
    def __init__(self, transmission):
        self.transmission = transmission
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self.transmission == 'kafka' or self.transmission == 'kafka_pdu':
            # Kafka Producer
            self.KAFKA_TOPIC = 'dis'
            self.producer = kp.KafkaProducer('localhost:9092', self.KAFKA_TOPIC)


        # Create garage dataset and timesteps for simulation
        thermoDataset = ton.TON_IoT_Datagen(dataset = 'thermostat')
        self.thermoTrain, self.thermoTest = thermoDataset.create_dataset(train_stepsize=thermoDataset.thermoTrainStepsize, test_stepsize=thermoDataset.thermoTestStepsize, 
                                        train= thermoDataset.completeThermoTrainSet, test = thermoDataset.completeThermoTestSet)

    def sendThermostatTrain(self):
        columnNames = self.thermoTrain['Dataframe'].columns
        # print(self.thermoTrain['Dataframe'].head())
        for i in range(len(self.thermoTrain['Data'][0])):
            if self.transmission == 'pdu':
                thermostatPdu = Environment()
                device = "Thermostat"
                thermostatPdu.device = device.encode('utf-8') # device 
                thermostatPdu.temperature = self.thermoTrain['Data'][0][i][0][3] # temperature
                thermostatPdu.temp_status = self.thermoTrain['Data'][0][i][0][4] #temp status
                thermostatPdu.attack = self.thermoTrain['Data'][0][i][0][5].encode('utf-8') # attack
                thermostatPdu.label = self.thermoTrain['Data'][0][i][0][6]
            
                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                thermostatPdu.serialize(outputStream)
                data = memoryStream.getvalue() 

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(thermostatPdu.__class__.__name__, len(data))
                    + "\n Thermostat Data Sent:"
                    + "\n  Device             : {}".format(thermostatPdu.device.decode('utf-8'))
                    + "\n  Temperature        : {}".format(thermostatPdu.temperature)
                    + "\n  Temp Status        : {}".format(thermostatPdu.temp_status)
                    + "\n  Attack             : {}".format(thermostatPdu.attack.decode('utf-8'))
                    + "\n  Label              : {}".format(thermostatPdu.label)
                    )
                
                time.sleep(16)

            elif self.transmission == 'kafka':
                #create an xml element for data
                root = ET.Element('ThermostatData')
                ET.SubElement(root, 'Temperature').text = str(self.thermoTrain['Data'][0][i][0][3])
                ET.SubElement(root, 'TempStatus').text = str(self.thermoTrain['Data'][0][i][0][4])
                ET.SubElement(root, 'Attack').text = str(self.thermoTrain['Data'][0][i][0][5])
                ET.SubElement(root, 'Label').text = str(self.thermoTrain['Data'][0][i][0][6])

                #create a new XML file with the results
                xml_data = ET.tostring(root, encoding='utf8')
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("ThermostatData", len(xml_data))
                    + "\n Thermostat Data Sent:"
                    + "\n  Temperature        : {}".format(self.thermoTest['Data'][0][i][0][3])
                    + "\n  Temp Status        : {}".format(self.thermoTest['Data'][0][i][0][4])
                    + "\n  Attack             : {}".format(self.thermoTest['Data'][0][i][0][5])
                    + "\n  Label              : {}\n".format(self.thermoTest['Data'][0][i][0][6])
                    ) 
                
                time.sleep(16)

            elif self.transmission == 'kafka_pdu':
                thermostatPdu = Environment()
                device = "Thermostat"
                thermostatPdu.device = device.encode('utf-8') # device 
                thermostatPdu.temperature = self.thermoTrain['Data'][0][i][0][3] # temperature
                thermostatPdu.temp_status = self.thermoTrain['Data'][0][i][0][4] #temp status
                thermostatPdu.attack = self.thermoTrain['Data'][0][i][0][5].encode('utf-8') # attack
                thermostatPdu.label = self.thermoTrain['Data'][0][i][0][6]
            
                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                thermostatPdu.serialize(outputStream)
                data = memoryStream.getvalue() 

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(thermostatPdu.__class__.__name__, len(data))
                    + "\n Thermostat Data Sent:"
                    + "\n  Device             : {}".format(thermostatPdu.device.decode('utf-8'))
                    + "\n  Temperature        : {}".format(thermostatPdu.temperature)
                    + "\n  Temp Status        : {}".format(thermostatPdu.temp_status)
                    + "\n  Attack             : {}".format(thermostatPdu.attack.decode('utf-8'))
                    + "\n  Label              : {}".format(thermostatPdu.label)
                    )
                
                time.sleep(16)

    def sendThermostatTest(self ):
        columnNames = self.thermoTest['Dataframe'].columns
        # print(self.thermoTest['Dataframe'].head())
        for i in range(len(self.thermoTrain['Data'][0])):
            if self.transmission == 'pdu':
                thermostatPdu = Environment()
                device = "Thermostat"
                thermostatPdu.device = device.encode('utf-8') # device 
                thermostatPdu.temperature = self.thermoTest['Data'][0][i][0][3] # temperature
                thermostatPdu.temp_status = self.thermoTest['Data'][0][i][0][4] #temp status
                thermostatPdu.attack = self.thermoTest['Data'][0][i][0][5].encode('utf-8') # attack
                thermostatPdu.label = self.thermoTest['Data'][0][i][0][6]
            
                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                thermostatPdu.serialize(outputStream)
                data = memoryStream.getvalue() 

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(thermostatPdu.__class__.__name__, len(data))
                    + "\n Thermostat Data Sent:"
                    + "\n  Device             : {}".format(thermostatPdu.device.decode('utf-8'))
                    + "\n  Temperature        : {}".format(thermostatPdu.temperature)
                    + "\n  Temp Status        : {}".format(thermostatPdu.temp_status)
                    + "\n  Attack             : {}".format(thermostatPdu.attack.decode('utf-8'))
                    + "\n  Label              : {}".format(thermostatPdu.label)
                    )
                 
                time.sleep(16)

            elif self.transmission == 'kafka':
                #create an xml element for data
                root = ET.Element('ThermostatData')
                ET.SubElement(root, 'Temperature').text = str(self.thermoTest['Data'][0][i][0][3])
                ET.SubElement(root, 'TempStatus').text = str(self.thermoTest['Data'][0][i][0][4])
                ET.SubElement(root, 'Attack').text = str(self.thermoTest['Data'][0][i][0][5])
                ET.SubElement(root, 'Label').text = str(self.thermoTest['Data'][0][i][0][6])

                #create a new XML file with the results
                xml_data = ET.tostring(root, encoding='utf8')
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("ThermostatData", len(xml_data))
                    + "\n Thermostat Data Sent:"
                    + "\n  Temperature        : {}".format(self.thermoTest['Data'][0][i][0][3])
                    + "\n  Temp Status        : {}".format(self.thermoTest['Data'][0][i][0][4])
                    + "\n  Attack             : {}".format(self.thermoTest['Data'][0][i][0][5])
                    + "\n  Label              : {}\n".format(self.thermoTest['Data'][0][i][0][6])
                    ) 
                
                time.sleep(16)
            
            elif self.transmission == 'kafka_pdu':
                thermostatPdu = Environment()
                device = "Thermostat"
                thermostatPdu.device = device.encode('utf-8') # device 
                thermostatPdu.temperature = self.thermoTest['Data'][0][i][0][3] # temperature
                thermostatPdu.temp_status = self.thermoTest['Data'][0][i][0][4] #temp status
                thermostatPdu.attack = self.thermoTest['Data'][0][i][0][5].encode('utf-8') # attack
                thermostatPdu.label = self.thermoTest['Data'][0][i][0][6]
            
                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                thermostatPdu.serialize(outputStream)
                data = memoryStream.getvalue() 

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(thermostatPdu.__class__.__name__, len(data))
                    + "\n Thermostat Data Sent:"
                    + "\n  Device             : {}".format(thermostatPdu.device.decode('utf-8'))
                    + "\n  Temperature        : {}".format(thermostatPdu.temperature)
                    + "\n  Temp Status        : {}".format(thermostatPdu.temp_status)
                    + "\n  Attack             : {}".format(thermostatPdu.attack.decode('utf-8'))
                    + "\n  Label              : {}".format(thermostatPdu.label)
                    )
                 
                time.sleep(16)

# if __name__ == '__main__':
#     thermostat = ThermostatSim(transmission= 'kafka_pdu')
#     thermostat.sendThermostatTrain()
