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

class AttackSim:

    def __init__(self, transmission):
        self.transmission = transmission
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self.transmission == 'kafka' or self.transmission == 'kafka_pdu':
            # Kafka Producer
            self.KAFKA_TOPIC = 'attack'
            self.producer = kp.KafkaProducer('localhost:9092', self.KAFKA_TOPIC)
        

        # Create garage dataset and timesteps for simulation
        attackDataset = ton.TON_IoT_Datagen(dataset = 'attack')
        self.attackTrain, self.attackTest = attackDataset.create_dataset(train_stepsize=attackDataset.attackTrainStepsize, test_stepsize=attackDataset.attackTestStepsize, 
                                        train= attackDataset.completeAttackTrainSet, test = attackDataset.completeAttackTestSet)

    def sendAttackTrain(self):
        columnNames = self.attackTrain['Dataframe'].columns
        # print(self.lightTrain['Dataframe'].head())
        for i in range(len(self.attackTrain['Data'][0])):
            if self.transmission == 'pdu':
                attackTrainPdu = Attack()
                attackTrainPdu.label = self.attackTrain['Data'][0][i][0][3] 
                attackTrainPdu.connections_info = self.attackTrain['Data'][0][i][0][4] 
                attackTrainPdu.num_handles = self.attackTrain['Data'][0][i][0][5]
                attackTrainPdu.nonpaged_pool = self.attackTrain['Data'][0][i][0][6]
                attackTrainPdu.pagefile = self.attackTrain['Data'][0][i][0][7] 
                attackTrainPdu.paged_pool = self.attackTrain['Data'][0][i][0][8]
                attackTrainPdu.peak_pagefile = self.attackTrain['Data'][0][i][0][9]
                attackTrainPdu.peak_nonpaged_pool = self.attackTrain['Data'][0][i][0][10]
                attackTrainPdu.read_count = self.attackTrain['Data'][0][i][0][11] 
                attackTrainPdu.user = self.attackTrain['Data'][0][i][0][12] 
                attackTrainPdu.system = self.attackTrain['Data'][0][i][0][13]
                attackTrainPdu.nice = self.attackTrain['Data'][0][i][0][14]
                attackTrainPdu.wset = self.attackTrain['Data'][0][i][0][15]
                attackTrainPdu.private = self.attackTrain['Data'][0][i][0][16]
                attackTrainPdu.cnt = self.attackTrain['Data'][0][i][0][17]
                attackTrainPdu.num_page_faults = self.attackTrain['Data'][0][i][0][18]
                attackTrainPdu.vms = self.attackTrain['Data'][0][i][0][19] 
                attackTrainPdu.memory_percent = self.attackTrain['Data'][0][i][0][20] 
                attackTrainPdu.rss = self.attackTrain['Data'][0][i][0][21]
                attackTrainPdu.read_bytes = self.attackTrain['Data'][0][i][0][22]
                attackTrainPdu.write_bytes = self.attackTrain['Data'][0][i][0][23] 
                attackTrainPdu.write_count = self.attackTrain['Data'][0][i][0][24] 
                attackTrainPdu.ionice = self.attackTrain['Data'][0][i][0][25]
                attackTrainPdu.other_count = self.attackTrain['Data'][0][i][0][26]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                attackTrainPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(attackTrainPdu.__class__.__name__, len(data)) 
                    + "\n Attack Data Sent:"
                    + " connections_info: {}\n".format(pdu.connections_info)
                    + " num_handles: {}\n".format(pdu.num_handles)
                    + " nonpaged_pool: {}\n".format(pdu.nonpaged_pool)
                    + " pagefile: {}\n".format(pdu.pagefile)
                    + " paged_pool: {}\n".format(pdu.paged_pool)
                    + " peak_pagefile: {}\n".format(pdu.peak_pagefile)
                    + " peak_nonpaged_pool: {}\n".format(pdu.peak_nonpaged_pool)
                    + " read_count: {}\n".format(pdu.read_count)
                    + " user: {}\n".format(pdu.user)
                    + " system: {}\n".format(pdu.system)
                    + " nice: {}\n".format(pdu.nice)
                    + " wset: {}\n".format(pdu.wset)
                    + " private: {}\n".format(pdu.private)
                    + " cnt: {}\n".format(pdu.cnt)
                    + " num_page_faults: {}\n".format(pdu.num_page_faults)
                    + " vms: {}\n".format(pdu.vms)
                    + " memory_percent: {}\n".format(pdu.memory_percent)
                    + " rss: {}\n".format(pdu.rss)
                    + " read_bytes: {}\n".format(pdu.read_bytes)
                    + " write_bytes: {}\n".format(pdu.write_bytes)
                    + " write_count: {}\n".format(pdu.write_count)
                    + " ionice: {}\n".format(pdu.ionice)
                    + " other_count: {}\n".format(pdu.other_count)
                    + " Attack: {}\n".format(pdu.label.decode('utf-8'))
                    + " Label : {}\n".format('TCIS computer')
                    )


# 'label', 'connections_info', 'num_handles', 'nonpaged_pool', 'pagefile',
#        'paged_pool', 'peak_pagefile', 'peak_nonpaged_pool', 'read_count',
#        'user', 'system', 'nice', 'wset', 'private', 'cnt', 'num_page_faults',
#        'vms', 'memory_percent', 'rss', 'read_bytes', 'write_bytes',
#        'write_count', 'ionice', 'other_count'
                time.sleep(random.uniform(0, 2))
            
            if self.transmission == 'kafka':
                # Create an XML element for each row in the dataframe
                root = ET.Element('AttackData')
                ET.SubElement(root, 'MotionStatus').text = str(self.attackTrain['Data'][0][i][0][3])
                ET.SubElement(root, 'AttackStatus').text = str(self.attackTrain['Data'][0][i][0][4])
                ET.SubElement(root, 'Attack').text = str(self.attackTrain['Data'][0][i][0][5])
                ET.SubElement(root, 'Label').text = str(self.attackTrain['Data'][0][i][0][6])

                # Create XML string
                xml_data = ET.tostring(root, encoding='utf8')

                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("AttackData", len(xml_data))
                    + "\n Attack Data Sent:"
                    + "\n  Motion Status : {}".format(self.attackTrain['Data'][0][i][0][3])
                    + "\n  Attack Status  : {}".format(self.attackTrain['Data'][0][i][0][4])
                    + "\n  Attack        : {}".format(self.attackTrain['Data'][0][i][0][5])
                    + "\n  Label         : {}\n".format(self.attackTrain['Data'][0][i][0][6])
                    )
                
                time.sleep(random.uniform(0, 2))

            if self.transmission == 'kafka_pdu':
                attackTrainPdu = Attack()
                attackTrainPdu.motion_status = self.attackTrain['Data'][0][i][0][3] # motion status
                attackTrainPdu.attack_status = self.attackTrain['Data'][0][i][0][4].encode() #light status
                attackTrainPdu.attack = self.attackTrain['Data'][0][i][0][5].encode()
                attackTrainPdu.label = self.attackTrain['Data'][0][i][0][6]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                attackTrainPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(attackTrainPdu.__class__.__name__, len(data)) 
                    + "\n Attack Data Sent:"
                    + "\n  Motion Status : {}".format(attackTrainPdu.motion_status)
                    + "\n  Attack Status  : {}".format(attackTrainPdu.light_status.decode('utf-8'))
                    + "\n  Attack        : {}".format(attackTrainPdu.attack.decode('utf-8'))
                    + "\n  Label         : {}\n".format(attackTrainPdu.label)
                    )

                time.sleep(random.uniform(0, 2))

    def sendAttackTest(self):
        columnNames = self.attackTest['Dataframe'].columns
        # print(self.lightTest['Dataframe'].head())
        for i in range(len(self.attackTrain['Data'][0])):
            if self.transmission == 'pdu':
                attackTrainPdu = Attack()
                attackTrainPdu.label = self.attackTrain['Data'][0][i][0][3] 
                attackTrainPdu.connections_info = self.attackTrain['Data'][0][i][0][4] 
                attackTrainPdu.num_handles = self.attackTrain['Data'][0][i][0][5]
                attackTrainPdu.nonpaged_pool = self.attackTrain['Data'][0][i][0][6]
                attackTrainPdu.pagefile = self.attackTrain['Data'][0][i][0][7] 
                attackTrainPdu.paged_pool = self.attackTrain['Data'][0][i][0][8]
                attackTrainPdu.peak_pagefile = self.attackTrain['Data'][0][i][0][9]
                attackTrainPdu.peak_nonpaged_pool = self.attackTrain['Data'][0][i][0][10]
                attackTrainPdu.read_count = self.attackTrain['Data'][0][i][0][11] 
                attackTrainPdu.user = self.attackTrain['Data'][0][i][0][12] 
                attackTrainPdu.system = self.attackTrain['Data'][0][i][0][13]
                attackTrainPdu.nice = self.attackTrain['Data'][0][i][0][14]
                attackTrainPdu.wset = self.attackTrain['Data'][0][i][0][15]
                attackTrainPdu.private = self.attackTrain['Data'][0][i][0][16]
                attackTrainPdu.cnt = self.attackTrain['Data'][0][i][0][17]
                attackTrainPdu.num_page_faults = self.attackTrain['Data'][0][i][0][18]
                attackTrainPdu.vms = self.attackTrain['Data'][0][i][0][19] 
                attackTrainPdu.memory_percent = self.attackTrain['Data'][0][i][0][20] 
                attackTrainPdu.rss = self.attackTrain['Data'][0][i][0][21]
                attackTrainPdu.read_bytes = self.attackTrain['Data'][0][i][0][22]
                attackTrainPdu.write_bytes = self.attackTrain['Data'][0][i][0][23] 
                attackTrainPdu.write_count = self.attackTrain['Data'][0][i][0][24] 
                attackTrainPdu.ionice = self.attackTrain['Data'][0][i][0][25]
                attackTrainPdu.other_count = self.attackTrain['Data'][0][i][0][26]

                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                attackTrainPdu.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))

                print("Sent {} PDU: {} bytes".format(attackTrainPdu.__class__.__name__, len(data)) 
                    + "\n Attack Data Sent:"
                    + " connections_info: {}\n".format(pdu.connections_info)
                    + " num_handles: {}\n".format(pdu.num_handles)
                    + " nonpaged_pool: {}\n".format(pdu.nonpaged_pool)
                    + " pagefile: {}\n".format(pdu.pagefile)
                    + " paged_pool: {}\n".format(pdu.paged_pool)
                    + " peak_pagefile: {}\n".format(pdu.peak_pagefile)
                    + " peak_nonpaged_pool: {}\n".format(pdu.peak_nonpaged_pool)
                    + " read_count: {}\n".format(pdu.read_count)
                    + " user: {}\n".format(pdu.user)
                    + " system: {}\n".format(pdu.system)
                    + " nice: {}\n".format(pdu.nice)
                    + " wset: {}\n".format(pdu.wset)
                    + " private: {}\n".format(pdu.private)
                    + " cnt: {}\n".format(pdu.cnt)
                    + " num_page_faults: {}\n".format(pdu.num_page_faults)
                    + " vms: {}\n".format(pdu.vms)
                    + " memory_percent: {}\n".format(pdu.memory_percent)
                    + " rss: {}\n".format(pdu.rss)
                    + " read_bytes: {}\n".format(pdu.read_bytes)
                    + " write_bytes: {}\n".format(pdu.write_bytes)
                    + " write_count: {}\n".format(pdu.write_count)
                    + " ionice: {}\n".format(pdu.ionice)
                    + " other_count: {}\n".format(pdu.other_count)
                    + " Attack: {}\n".format(pdu.label.decode('utf-8'))
                    + " Label : {}\n".format('TCIS computer')
                    )
                time.sleep(random.uniform(0, 2))
            
            if self.transmission == 'kafka':
                # Create an XML element for each row in the dataframe
                root = ET.Element('AttackData')
                ET.SubElement(root, 'MotionStatus').text = str(self.lightTest['Data'][0][i][0][3])
                ET.SubElement(root, 'AttackStatus').text = str(self.lightTest['Data'][0][i][0][4])
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

                print("Sent {} PDU: {} bytes".format(attackPdu.__class__.__name__, len(data)) 
                    + "\n Attack Data Sent:"
                    + "\n  Motion Status : {}".format(attackPdu.motion_status)
                    + "\n  Attack Status  : {}".format(attackPdu.light_status.decode('utf-8'))
                    + "\n  Attack        : {}".format(attackPdu.attack.decode('utf-8'))
                    + "\n  Label         : {}\n".format(attackPdu.label)
                    )
                
                time.sleep(random.uniform(0, 2))


if __name__ == '__main__':
    AttackSim = AttackSim(transmission= 'pdu')
    # AttackSim = AttackSim(transmission= 'kafka_pdu')
    LightSim.sendLightTrain()
