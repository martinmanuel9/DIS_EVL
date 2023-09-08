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

DP_PORT = 3001
DESTINATION_ADDRESS = "127.0.0.1"

udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

# Create garage dataset and timesteps for simulation
thermoDataset = ton.TON_IoT_Datagen()
thermoTrain, thermoTest = thermoDataset.create_dataset(train_stepsize=thermoDataset.thermoTrainStepsize, test_stepsize=thermoDataset.thermoTestStepsize, 
                                train= thermoDataset.completeThermoTrainSet, test = thermoDataset.completeThermoTestSet)

def sendTrainTemp():
    columnNames = thermoTrain['Dataframe'].columns
    for i in range(len(thermoTrain['Data'][0])):
        
        envTempPdu = Environment()
        envTempPdu.environmentType = int(thermoTrain['Data'][0][i][0][3])
        envTempPdu.length = sys.getsizeof(thermoTrain['Data'][0][i][0][3]) 
        envTempPdu.index = 0 


        memoryStream = BytesIO()
        outputStream = DataOutputStream(memoryStream)
        envTempPdu.serialize(outputStream)
        data = memoryStream.getvalue()

        udpSocket.sendto(data, (DESTINATION_ADDRESS, DP_PORT))
        print('Train Data Temperature: ', thermoTrain['Data'][0][i][0][3]) # temperature
        print('Packet Size: ' , sys.getsizeof(thermoTrain['Data'][0][i][0][3]))
        print("Sent {}: {} bytes".format(envTempPdu.__class__.__name__, len(data)))
        time.sleep(10)

sendTrainTemp()


    