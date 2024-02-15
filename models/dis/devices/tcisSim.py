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
#from evl import ton_iot_dis_datagen as ton
from opendismodel.opendis.dis7 import * 
from opendismodel.opendis.DataOutputStream import DataOutputStream
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import KafkaProducer as kp
import xml.etree.ElementTree as ET

class TcisSim:

    def __init__(self, transmission):
        self.transmission = transmission
        self.UDP_PORT = 3001
        self.DESTINATION_ADDRESS = "127.0.0.1"

        self.udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        if self.transmission == 'kafka' or self.transmission == 'kafka_pdu':
            # Kafka Producer
            self.KAFKA_TOPIC = 'data_ivan_1'
            self.producer = kp.KafkaProducer('localhost:9092', self.KAFKA_TOPIC)
        

        # Create garage dataset and timesteps for simulation
        # attackDataset = ton.TON_IoT_Datagen(dataset = 'attack')
        # self.attackTrain, self.attackTest = attackDataset.create_dataset(train_stepsize=attackDataset.attackTrainStepsize, test_stepsize=attackDataset.attackTestStepsize, 
        #                                 train= attackDataset.completeAttackTrainSet, test = attackDataset.completeAttackTestSet)


    def sendAttack(self, df):
        columnNames = df.columns
        # print(self.lightTrain['Dataframe'].head())
        for i, row in df.iterrows():
            if self.transmission == 'pdu':
                tcis = TCIS()
                
                tcis.connections_info = row['connections_info'] 
                tcis.num_handles = row['num_handles']
                tcis.nonpaged_pool = row['nonpaged_pool']
                tcis.pagefile = row['pagefile'] 
                tcis.paged_pool = row['paged_pool']
                tcis.peak_pagefile = row['peak_pagefile']
                tcis.peak_nonpaged_pool = row['peak_nonpaged_pool']
                tcis.read_count = row['read_count'] 
                tcis.user = row['user'] 
                tcis.system = row['system']
                tcis.nice = row['nice']
                tcis.wset = row['wset']
                tcis.private = row['private']
                tcis.cnt = row['cnt']
                tcis.num_page_faults = row['num_page_faults']
                tcis.vms =row['vms'] 
                tcis.memory_percent = row['memory_percent'] 
                tcis.rss =row['rss']
                tcis.read_bytes = row['read_bytes']
                tcis.write_bytes = row['write_bytes'] 
                tcis.write_count = row['write_count'] 
                tcis.ionice = row['ionice']
                tcis.other_count = row['other_count']
                tcis.label = row['label'].encode('utf-8')
                
                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                tcis.serialize(outputStream)
                data = memoryStream.getvalue()

                self.udpSocket.sendto(data, (self.DESTINATION_ADDRESS, self.UDP_PORT))
                print("Sent {} PDU: {} bytes".format(tcis.__class__.__name__, len(data)) 
                    + "\n Attack Data Sent:"
                    + " connections_info: {}\n".format(tcis.connections_info)
                    + " num_handles: {}\n".format(tcis.num_handles)
                    + " nonpaged_pool: {}\n".format(tcis.nonpaged_pool)
                    + " pagefile: {}\n".format(tcis.pagefile)
                    + " paged_pool: {}\n".format(tcis.paged_pool)
                    + " peak_pagefile: {}\n".format(tcis.peak_pagefile)
                    + " peak_nonpaged_pool: {}\n".format(tcis.peak_nonpaged_pool)
                    + " read_count: {}\n".format(tcis.read_count)
                    + " user: {}\n".format(tcis.user)
                    + " system: {}\n".format(tcis.system)
                    + " nice: {}\n".format(tcis.nice)
                    + " wset: {}\n".format(tcis.wset)
                    + " private: {}\n".format(tcis.private)
                    + " cnt: {}\n".format(tcis.cnt)
                    + " num_page_faults: {}\n".format(tcis.num_page_faults)
                    + " vms: {}\n".format(tcis.vms)
                    + " memory_percent: {}\n".format(tcis.memory_percent)
                    + " rss: {}\n".format(tcis.rss)
                    + " read_bytes: {}\n".format(tcis.read_bytes)
                    + " write_bytes: {}\n".format(tcis.write_bytes)
                    + " write_count: {}\n".format(tcis.write_count)
                    + " ionice: {}\n".format(tcis.ionice)
                    + " other_count: {}\n".format(tcis.other_count)
                    + " Attack: {}\n".format(tcis.label.decode('utf-8'))
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
                ET.SubElement(root, 'connections_info').text = str(row['connections_info'])
                ET.SubElement(root, 'nonpaged_pool').text = str(row['nonpaged_pool'])
                ET.SubElement(root, 'pagefile').text = str(row['pagefile'])
                ET.SubElement(root, 'paged_pool').text = str(row['paged_pool'])
                ET.SubElement(root, 'num_handles').text = str(row['num_handles'])
                ET.SubElement(root, 'nonpaged_pool').text = str(row['nonpaged_pool'])
                ET.SubElement(root, 'pagefile').text = str(row['pagefile'])
                ET.SubElement(root, 'paged_pool').text = str(row['paged_pool'])
                ET.SubElement(root, 'peak_pagefile').text = str(row['peak_pagefile'])
                ET.SubElement(root, 'peak_nonpaged_pool').text = str(row['peak_nonpaged_pool'])
                ET.SubElement(root, 'read_count').text = str(row['read_count'])
                ET.SubElement(root, 'user').text = str(row['user'])
                ET.SubElement(root, 'system').text = str(row['system'])
                ET.SubElement(root, 'nice').text = str(row['nice'])
                ET.SubElement(root, 'wset').text = str(row['wset'])
                ET.SubElement(root, 'private').text = str(row['private'])
                ET.SubElement(root, 'cnt').text = str(row['cnt'])
                ET.SubElement(root, 'num_page_faults').text = str(row['num_page_faults'])
                ET.SubElement(root, 'vms').text = str(row['vms'])
                ET.SubElement(root, 'memory_percent').text = str(row['memory_percent'])
                ET.SubElement(root, 'rss').text = str(row['rss'])
                ET.SubElement(root, 'read_bytes').text = str(row['read_bytes'])
                ET.SubElement(root, 'write_bytes').text = str(row['write_bytes'])
                ET.SubElement(root, 'write_count').text = str(row['write_count'])
                ET.SubElement(root, 'ionice').text = str(row['ionice'])
                ET.SubElement(root, 'other_count').text = str(row['other_count'])
                ET.SubElement(root, 'label').text = str(row['label'])

                xml_data = ET.tostring(root, encoding='utf8')

                self.producer.produce_message(xml_data)

                print("Sent {} PDU: {} bytes".format("AttackData", len(xml_data))
                    + "\n Attack Data Sent:"
                    + " connections_info: {}\n".format(tcis.connections_info)
                    + " num_handles: {}\n".format(tcis.num_handles)
                    + " nonpaged_pool: {}\n".format(tcis.nonpaged_pool)
                    + " pagefile: {}\n".format(tcis.pagefile)
                    + " paged_pool: {}\n".format(tcis.paged_pool)
                    + " peak_pagefile: {}\n".format(tcis.peak_pagefile)
                    + " peak_nonpaged_pool: {}\n".format(tcis.peak_nonpaged_pool)
                    + " read_count: {}\n".format(tcis.read_count)
                    + " user: {}\n".format(tcis.user)
                    + " system: {}\n".format(tcis.system)
                    + " nice: {}\n".format(tcis.nice)
                    + " wset: {}\n".format(tcis.wset)
                    + " private: {}\n".format(tcis.private)
                    + " cnt: {}\n".format(tcis.cnt)
                    + " num_page_faults: {}\n".format(tcis.num_page_faults)
                    + " vms: {}\n".format(tcis.vms)
                    + " memory_percent: {}\n".format(tcis.memory_percent)
                    + " rss: {}\n".format(tcis.rss)
                    + " read_bytes: {}\n".format(tcis.read_bytes)
                    + " write_bytes: {}\n".format(tcis.write_bytes)
                    + " write_count: {}\n".format(tcis.write_count)
                    + " ionice: {}\n".format(tcis.ionice)
                    + " other_count: {}\n".format(tcis.other_count)
                    + " Attack: {}\n".format(tcis.label.decode('utf-8'))
                    + " Label : {}\n".format('TCIS computer')
                    )
                
                time.sleep(random.uniform(0, 2))

            if self.transmission == 'kafka_pdu':
                tcis = TCIS()
                
                tcis.connections_info = row['connections_info'] 
                tcis.num_handles = row['num_handles']
                tcis.nonpaged_pool = row['nonpaged_pool']
                tcis.pagefile = row['pagefile'] 
                tcis.paged_pool = row['paged_pool']
                tcis.peak_pagefile = row['peak_pagefile']
                tcis.peak_nonpaged_pool = row['peak_nonpaged_pool']
                tcis.read_count = row['read_count'] 
                tcis.user = row['user'] 
                tcis.system = row['system']
                tcis.nice = row['nice']
                tcis.wset = row['wset']
                tcis.private = row['private']
                tcis.cnt = row['cnt']
                tcis.num_page_faults = row['num_page_faults']
                tcis.vms =row['vms'] 
                tcis.memory_percent = row['memory_percent'] 
                tcis.rss =row['rss']
                tcis.read_bytes = row['read_bytes']
                tcis.write_bytes = row['write_bytes'] 
                tcis.write_count = row['write_count'] 
                tcis.ionice = row['ionice']
                tcis.other_count = row['other_count']
                tcis.label = row['label'].encode('utf-8')
                
                memoryStream = BytesIO()
                outputStream = DataOutputStream(memoryStream)
                tcis.serialize(outputStream)
                data = memoryStream.getvalue()

                self.producer.produce_message(data)

                print("Sent {} PDU: {} bytes".format(tcis.__class__.__name__, len(data)) 
                    + "\n Attack Data Sent:"
                    + " connections_info: {}\n".format(tcis.connections_info)
                    + " num_handles: {}\n".format(tcis.num_handles)
                    + " nonpaged_pool: {}\n".format(tcis.nonpaged_pool)
                    + " pagefile: {}\n".format(tcis.pagefile)
                    + " paged_pool: {}\n".format(tcis.paged_pool)
                    + " peak_pagefile: {}\n".format(tcis.peak_pagefile)
                    + " peak_nonpaged_pool: {}\n".format(tcis.peak_nonpaged_pool)
                    + " read_count: {}\n".format(tcis.read_count)
                    + " user: {}\n".format(tcis.user)
                    + " system: {}\n".format(tcis.system)
                    + " nice: {}\n".format(tcis.nice)
                    + " wset: {}\n".format(tcis.wset)
                    + " private: {}\n".format(tcis.private)
                    + " cnt: {}\n".format(tcis.cnt)
                    + " num_page_faults: {}\n".format(tcis.num_page_faults)
                    + " vms: {}\n".format(tcis.vms)
                    + " memory_percent: {}\n".format(tcis.memory_percent)
                    + " rss: {}\n".format(tcis.rss)
                    + " read_bytes: {}\n".format(tcis.read_bytes)
                    + " write_bytes: {}\n".format(tcis.write_bytes)
                    + " write_count: {}\n".format(tcis.write_count)
                    + " ionice: {}\n".format(tcis.ionice)
                    + " other_count: {}\n".format(tcis.other_count)
                    + " Attack: {}\n".format(tcis.label.decode('utf-8'))
                    + " Label : {}\n".format('TCIS computer')
                    )

                time.sleep(random.uniform(0, 2))


if __name__ == '__main__':
    df = pd.read_csv('~/DIS_EVL/data/attack_dis_sim.csv')
    TcisSim = TcisSim(transmission= 'pdu')
    #TcisSim = TcisSim(transmission= 'kafka_pdu')
    TcisSim.sendAttack(df)
