#!/usr/bin/env python 

"""
Application:        DIS Simulation of Modbus Model 
File name:          modbusSim.py
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
import dataOps.ton_iot_dis_datagen as ton
import KafkaProducer as kp
import xml.etree.ElementTree as ET

class ModbusSim:

    def __init__(self, transmission, speed):
        self.transmission = transmission
        self.speed = speed
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self.transmission == 'kafka' or self.transmission == 'kafka_pdu':
            # Kafka Producer
            self.KAFKA_TOPIC = 'modbus'
            self.producer = kp.KafkaProducer('172.18.0.4:9092', self.KAFKA_TOPIC)

        # Create garage dataset and timesteps for simulation
        modbusDataset = ton.TON_IoT_Datagen(dataset= 'modbus')
        self.modbusTrain, self.modbusTest = modbusDataset.create_dataset(train_stepsize=modbusDataset.modbusTrainStepsize, test_stepsize=modbusDataset.modbusTestStepsize, 
                                        train= modbusDataset.completeModbusTrainSet, test = modbusDataset.completeModbusTestSet)


    def sendModbusTrain(self):
        columnNames = self.modbusTrain['Dataframe'].columns
        # print(self.modbusTrain['Dataframe'].head())
        for i in range(len(self.modbusTrain['Dataframe'])):
            if self.transmission == 'pdu':
                modbusPdu = Modbus() 
                modbusPdu.fc1 = self.modbusTrain['Dataframe']['FC1_Read_Input_Register'][i]
                modbusPdu.fc2 = self.modbusTrain['Dataframe']['FC2_Read_Discrete_Value'][i]
                modbusPdu.fc3 = self.modbusTrain['Dataframe']['FC3_Read_Holding_Register'][i]
                modbusPdu.fc4 = self.modbusTrain['Dataframe']['FC4_Read_Coil'][i]
                modbusPdu.attack = self.modbusTrain['Dataframe']['type'][i].encode()
                modbusPdu.label = self.modbusTrain['Dataframe']['label'][i]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                modbusPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(modbusPdu.__class__.__name__, len(data)) 
                    + "\n Modbus Data Sent:"
                    + "\n  FC1            : {}".format(modbusPdu.fc1)
                    + "\n  FC2            : {}".format(modbusPdu.fc2)
                    + "\n  FC3            : {}".format(modbusPdu.fc3)
                    + "\n  FC4            : {}".format(modbusPdu.fc4)
                    + "\n  Attack         : {}".format(modbusPdu.attack.decode('utf-8'))
                    + "\n  Label          : {}\n".format(modbusPdu.label)
                    ) 
                
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))

            if self.transmission == 'kafka':
                # Create an XML element for each row in the dataframe
                root = ET.Element('Modbus')
                ET.SubElement(root, 'fc1').text = str(self.modbusTrain['Dataframe']['FC1_Read_Input_Register'][i])
                ET.SubElement(root, 'fc2').text = str(self.modbusTrain['Dataframe']['FC2_Read_Discrete_Value'][i])
                ET.SubElement(root, 'fc3').text = str(self.modbusTrain['Dataframe']['FC3_Read_Holding_Register'][i])
                ET.SubElement(root, 'fc4').text = str(self.modbusTrain['Dataframe']['FC4_Read_Coil'][i])
                ET.SubElement(root, 'attack').text = str(self.modbusTrain['Dataframe']['type'][i])
                ET.SubElement(root, 'label').text = str(self.modbusTrain['Dataframe']['label'][i])

                # Create XML string
                xml_data = ET.tostring(root, encoding = 'utf8')
                
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("ModbusData", len(xml_data)) 
                    + "\n Modbus Data Sent:"
                    + "\n  FC1            : {}".format(self.modbusTrain['Dataframe']['FC1_Read_Input_Register'][i])
                    + "\n  FC2            : {}".format(self.modbusTrain['Dataframe']['FC2_Read_Discrete_Value'][i])
                    + "\n  FC3            : {}".format(self.modbusTrain['Dataframe']['FC3_Read_Holding_Register'][i])
                    + "\n  FC4            : {}".format(self.modbusTrain['Dataframe']['FC4_Read_Coil'][i])
                    + "\n  Attack         : {}".format(self.modbusTrain['Dataframe']['type'][i])
                    + "\n  Label          : {}\n".format(self.modbusTrain['Dataframe']['label'][i])
                    ) 
                    
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))

            if self.transmission == 'kafka_pdu':
                modbusPdu = Modbus() 
                modbusPdu.fc1 = self.modbusTrain['Dataframe']['FC1_Read_Input_Register'][i]
                modbusPdu.fc2 = self.modbusTrain['Dataframe']['FC2_Read_Discrete_Value'][i]
                modbusPdu.fc3 = self.modbusTrain['Dataframe']['FC3_Read_Holding_Register'][i]
                modbusPdu.fc4 = self.modbusTrain['Dataframe']['FC4_Read_Coil'][i]
                modbusPdu.attack = self.modbusTrain['Dataframe']['type'][i].encode()
                modbusPdu.label = self.modbusTrain['Dataframe']['label'][i]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                modbusPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(modbusPdu.__class__.__name__, len(data)) 
                    + "\n Modbus Data Sent:"
                    + "\n  FC1            : {}".format(modbusPdu.fc1)
                    + "\n  FC2            : {}".format(modbusPdu.fc2)
                    + "\n  FC3            : {}".format(modbusPdu.fc3)
                    + "\n  FC4            : {}".format(modbusPdu.fc4)
                    + "\n  Attack         : {}".format(modbusPdu.attack.decode('utf-8'))
                    + "\n  Label          : {}\n".format(modbusPdu.label)
                    ) 
                
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))

    def sendModbusTest(self ):
        columnNames = self.modbusTest['Dataframe'].columns
        # print(self.modbumodbusTestsTrain['Dataframe'].head())
        for i in range(len(self.modbusTrain['Dataframe'])):
            if self.transmission == 'pdu':
                modbusPdu = Modbus() 
                modbusPdu.fc1 = self.modbusTest['Dataframe']['FC1_Read_Input_Register'][i]
                modbusPdu.fc2 = self.modbusTest['Dataframe']['FC2_Read_Discrete_Value'][i]
                modbusPdu.fc3 = self.modbusTest['Dataframe']['FC3_Read_Holding_Register'][i]
                modbusPdu.fc4 = self.modbusTest['Dataframe']['FC4_Read_Coil'][i]
                modbusPdu.attack = self.modbusTest['Dataframe']['type'][i].encode()
                modbusPdu.label = self.modbusTest['Dataframe']['label'][i]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                modbusPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(modbusPdu.__class__.__name__, len(data)) 
                    + "\n Modbus Data Sent:"
                    + "\n  FC1            : {}".format(modbusPdu.fc1)
                    + "\n  FC2            : {}".format(modbusPdu.fc2)
                    + "\n  FC3            : {}".format(modbusPdu.fc3)
                    + "\n  FC4            : {}".format(modbusPdu.fc4)
                    + "\n  Attack         : {}".format(modbusPdu.attack.decode('utf-8'))
                    + "\n  Label          : {}\n".format(modbusPdu.label)
                    )  
                
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))

            if self.transmission == 'kafka':
                # Create an XML element for each row in the dataframe
                root = ET.Element('Modbus')
                ET.SubElement(root, 'fc1').text = str(self.modbusTest['Dataframe']['FC1_Read_Input_Register'][i])
                ET.SubElement(root, 'fc2').text = str(self.modbusTest['Dataframe']['FC2_Read_Discrete_Value'][i])
                ET.SubElement(root, 'fc3').text = str(self.modbusTest['Dataframe']['FC3_Read_Holding_Register'][i])
                ET.SubElement(root, 'fc4').text = str(self.modbusTest['Dataframe']['FC4_Read_Coil'][i])
                ET.SubElement(root, 'attack').text = str(self.modbusTest['Dataframe']['type'][i])
                ET.SubElement(root, 'label').text = str(self.modbusTest['Dataframe']['label'][i])

                # Create XML string
                xml_data = ET.tostring(root, encoding = 'utf8')
                
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("ModbusData", len(xml_data)) 
                    + "\n Modbus Data Sent:"
                    + "\n  FC1            : {}".format(self.modbusTest['Dataframe']['FC1_Read_Input_Register'][i])
                    + "\n  FC2            : {}".format(self.modbusTest['Dataframe']['FC2_Read_Discrete_Value'][i])
                    + "\n  FC3            : {}".format(self.modbusTest['Dataframe']['FC3_Read_Holding_Register'][i])
                    + "\n  FC4            : {}".format(self.modbusTest['Dataframe']['FC4_Read_Coil'][i])
                    + "\n  Attack         : {}".format(self.modbusTest['Dataframe']['type'][i])
                    + "\n  Label          : {}\n".format(self.modbusTest['Dataframe']['label'][i])
                    ) 
                
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))

            if self.transmission == 'kafka_pdu':
                modbusPdu = Modbus() 
                modbusPdu.fc1 = self.modbusTest['Dataframe']['FC1_Read_Input_Register'][i]
                modbusPdu.fc2 = self.modbusTest['Dataframe']['FC2_Read_Discrete_Value'][i]
                modbusPdu.fc3 = self.modbusTest['Dataframe']['FC3_Read_Holding_Register'][i]
                modbusPdu.fc4 = self.modbusTest['Dataframe']['FC4_Read_Coil'][i]
                modbusPdu.attack = self.modbusTest['Dataframe']['type'][i].encode()
                modbusPdu.label = self.modbusTest['Dataframe']['label'][i]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                modbusPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(modbusPdu.__class__.__name__, len(data)) 
                    + "\n Modbus Data Sent:"
                    + "\n  FC1            : {}".format(modbusPdu.fc1)
                    + "\n  FC2            : {}".format(modbusPdu.fc2)
                    + "\n  FC3            : {}".format(modbusPdu.fc3)
                    + "\n  FC4            : {}".format(modbusPdu.fc4)
                    + "\n  Attack         : {}".format(modbusPdu.attack.decode('utf-8'))
                    + "\n  Label          : {}\n".format(modbusPdu.label)
                    )  
                
                if self.speed == 'slow':
                    time.sleep(random.uniform(0, 3))

# if __name__ == '__main__':
#     modbusSim = ModbusSim(transmission = 'kafka_pdu', speed= 'fast')
#     modbusSim.sendModbusTrain()
    

