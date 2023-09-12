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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from evl import ton_iot_dis_datagen as ton
from opendismodel.opendis.dis7 import * 
from opendismodel.opendis.DataOutputStream import DataOutputStream

UDP_PORT = 3001
DESTINATION_ADDRESS = "127.0.0.1"

udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

# Create garage dataset and timesteps for simulation
lightDataset = ton.TON_IoT_Datagen()
lightTrain, lightTest = lightDataset.create_dataset(train_stepsize=lightDataset.lightTrainStepsize, test_stepsize=lightDataset.lightTestStepsize, 
                                train= lightDataset.completeLightTrainSet, test = lightDataset.completeLightTestSet)

def sendLightTrain():
    columnNames = lightTrain['Dataframe'].columns
    # print(lightTrain['Dataframe'].head())
    for i in range(len(lightTrain['Data'][0])):
        lightTrainPdu = FirePdu()
        lightTrainPdu.motion_statis = lightTrain['Data'][0][i][0][3] # motion status
        lightTrainPdu.light_status = lightTrain['Data'][0][i][0][4].encode() #light status
        lightTrainPdu.attack = lightTrain['Data'][0][i][0][5]
        lightTrainPdu.label = lightTrain['Data'][0][i][0][6]

        memoryStream = BytesIO()
        outputStream = DataOutputStream(memoryStream)
        lightTrainPdu.serialize(outputStream)
        data = memoryStream.getvalue()

        udpSocket.sendto(data, (DESTINATION_ADDRESS, UDP_PORT))

        print("Sent {}: {} bytes".format(lightTrainPdu.__class__.__name__, len(data)))
        time.sleep(24)

sendLightTrain()





sendLightTrain()