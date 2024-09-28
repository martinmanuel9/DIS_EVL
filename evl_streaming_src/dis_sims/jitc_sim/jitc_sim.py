#!/usr/bin/env python 

"""
Application:        DIS Simulation of JITC Data 
File name:          jitc_sim.py
Author:             Martin Manuel Lopez
Creation:           8/07/2024

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
import pickle
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from opendismodel.opendis.dis7 import *
from opendismodel.opendis.DataOutputStream import DataOutputStream
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import dataOps.ton_iot_dis_datagen as ton
import KafkaProducer as kp
import xml.etree.ElementTree as ET


class JITCSim:
    def __init__(self, transmission, speed):
        self.transmission = transmission
        self.speed = speed
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self.transmission == 'kafka' or self.transmission == 'kafka_pdu':
            # Kafka Producer
            self.KAFKA_TOPIC = 'jitc_data'
            self.producer = kp.KafkaProducer('172.18.0.4:9092', self.KAFKA_TOPIC)
        
        jitc_train_dataset_path = os.getcwd() + '/evl_streaming_src/datasets/JITC_Train_Number_Dataframe_Normalized.pkl'
        # Create garage dataset and timesteps for simulation
        with open(jitc_train_dataset_path, 'rb') as file:
            jitc_train_dataframe = pickle.load(file)
            
        
        df_bit_train_number = jitc_train_dataframe['bit_number']
        df_bit_train_number = pd.DataFrame(list(df_bit_train_number))
        df_train_labels = jitc_train_dataframe['labels']
        df_train_labels = pd.DataFrame(list(df_train_labels))
        
        # concat ngrams_freq and labels and keep column names and order of columns
        normal_dataset_train = pd.concat([df_bit_train_number, df_train_labels], axis=1)
        # last column is label
        normal_dataset_train.columns = list(df_bit_train_number.columns) + ['label']
        # reset index for dataframe normal dataset
        normal_dataset_train.reset_index(drop=True, inplace=True)
        # rename the first column to 'jitc_message'
        normal_dataset_train.rename(columns={0: 'jitc_normalized'}, inplace=True)
        self.jitc_train = normal_dataset_train
        # the self.jitc_train takes the normalized dataset
        
        jitc_test_dataset_path = os.getcwd() + '/evl_streaming_src/datasets/JITC_Test_Number_Dataframe_Normalized.pkl'
        # Create garage dataset and timesteps for simulation
        with open(jitc_test_dataset_path, 'rb') as test_file:
            jitc_test_dataframe = pickle.load(test_file)
            
        
        df_bit_test_number = jitc_test_dataframe['bit_number']
        df_bit_test_number = pd.DataFrame(list(df_bit_test_number))
        df_test_labels = jitc_test_dataframe['labels']
        df_test_labels = pd.DataFrame(list(df_test_labels))
        
        # concat ngrams_freq and labels and keep column names and order of columns
        normal_dataset_test = pd.concat([df_bit_test_number, df_test_labels], axis=1)
        # last column is label
        normal_dataset_test.columns = list(df_bit_test_number.columns) + ['label']
        # reset index for dataframe normal dataset
        normal_dataset_test.reset_index(drop=True, inplace=True)
        # rename the first column to 'jitc_message'
        normal_dataset_test.rename(columns={0: 'jitc_normalized'}, inplace=True)
        self.jitc_test = normal_dataset_test
        
    def sendJITC_Train(self ):
        columnNames = self.jitc_train.columns
        # print(self.fridgeTrain['Dataframe'].head())
        for i in range(len(self.jitc_train)):
            """Sending via PDU and UDP Protocol via Open DIS """
            if self.transmission == 'pdu':
                jitcPdu = JITC()
                jitcPdu.jitc_normalized = self.jitc_train['jitc_normalized'].iloc[i] # jitc message
                jitcPdu.label = self.jitc_train['label'].iloc[i]  #label

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                jitcPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU via UDP: {} bytes".format(jitcPdu.__class__.__name__, len(data))
                    + "\n JITC Data Sent:"
                    + "\n  JITC Message : {}".format(jitcPdu.jitc_normalized)
                    + "\n  Label        : {}\n".format(jitcPdu.label)
                )
                
                if self.speed == 'slow':
                    time.sleep(2)

            """Sending via Kafka Producer"""
            if self.transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("JITC_Data")
                ET.SubElement(root, "jitc_normalized").text = str(self.jitc_train['jitc_normalized'].iloc[i])
                ET.SubElement(root, "Label").text = str(self.jitc_train['label'].iloc[i])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf-8')

                # Send the XML data to Kafka
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("JITC_Data", len(xml_data))
                    + "\n JITC Data Sent:"
                    + "\n  JITC Message : {}".format(self.jitc_train['jitc_normalized'].iloc[i])
                    + "\n  Label        : {}\n".format(self.jitc_train['label'].iloc[i])
                    )
                
                if self.speed == 'slow':
                    time.sleep(2)
            
            if self.transmission == 'kafka_pdu':
                
                jitcPdu = JITC()
                jitcPdu.jitc_normalized = self.jitc_train['jitc_normalized'].iloc[i] # jitc message
                jitcPdu.label = self.jitc_train['label'].iloc[i]  #label

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                jitcPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)
                
                print("Sent {} PDU: {} bytes".format(jitcPdu.__class__.__name__, len(data))
                    + "\n JITC Data Sent:"
                    + "\n  JITC Message : {}".format(jitcPdu.jitc_normalized)
                    + "\n  Label        : {}\n".format(jitcPdu.label)
                )
                
                if self.speed == 'slow':
                    time.sleep(2)

    def sendJITCTest(self):
        columnNames = self.jitc_test.columns
        # print(self.fridgeTrain['Dataframe'].head())
        for i in range(len(self.jitc_test)):
            """Sending via PDU and UDP Protocol via Open DIS """
            if self.transmission == 'pdu':
                jitcPdu = JITC()
                jitcPdu.jitc_normalized = self.jitc_test['jitc_normalized'].iloc[i] # jitc message
                jitcPdu.label = self.jitc_test['label'].iloc[i]  #label

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                jitcPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU via UDP: {} bytes".format(jitcPdu.__class__.__name__, len(data))
                    + "\n JITC Data Sent:"
                    + "\n  JITC Message : {}".format(jitcPdu.jitc_normalized)
                    + "\n  Label        : {}\n".format(jitcPdu.label)
                )
                
                if self.speed == 'slow':
                    time.sleep(2)

            """Sending via Kafka Producer"""
            if self.transmission == 'kafka':
                # Create an XML element for the data
                root = ET.Element("JITC_Data")
                ET.SubElement(root, "jitc_normalized").text = str(self.jitc_test['jitc_normalized'].iloc[i])
                ET.SubElement(root, "Label").text = str(self.jitc_test['label'].iloc[i])

                # Convert the XML element to a string
                xml_data = ET.tostring(root, encoding='utf-8')

                # Send the XML data to Kafka
                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("JITC_Data", len(xml_data))
                    + "\n JITC Data Sent:"
                    + "\n  JITC Message : {}".format(self.jitc_train['jitc_normalized'].iloc[i])
                    + "\n  Label        : {}\n".format(self.jitc_train['label'].iloc[i])
                    )
                
                if self.speed == 'slow':
                    time.sleep(2)
            
            if self.transmission == 'kafka_pdu':
                
                jitcPdu = JITC()
                jitcPdu.jitc_normalized = self.jitc_test['jitc_normalized'].iloc[i] # jitc message
                jitcPdu.label = self.jitc_test['label'].iloc[i]  #label

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                jitcPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)
                
                print("Sent {} PDU: {} bytes".format(jitcPdu.__class__.__name__, len(data))
                    + "\n JITC Data Sent:"
                    + "\n  JITC Message : {}".format(jitcPdu.jitc_normalized)
                    + "\n  Label        : {}\n".format(jitcPdu.label)
                )
                
                if self.speed == 'slow':
                    time.sleep(2)
                


if __name__ == "__main__":
    jitc_sim = JITCSim(transmission= 'kafka_pdu', speed='slow')
    jitc_sim.sendJITCTest()
