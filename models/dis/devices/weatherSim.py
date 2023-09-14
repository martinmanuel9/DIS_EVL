#!/usr/bin/env python 

"""
Application:        DIS Simulation of Weather Model 
File name:          weatherSim.py
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
weatherDataset = ton.TON_IoT_Datagen()
weatherTrain, weatherTest = weatherDataset.create_dataset(train_stepsize=weatherDataset.weatherTrainStepsize, test_stepsize=weatherDataset.weatherTestStepsize, 
                                train= weatherDataset.completeWeatherTrainSet, test = weatherDataset.completeWeatherTestSet)

def sendWeatherTrain():
    columnNames = weatherTrain['Dataframe'].columns
    # print(weatherTrain['Dataframe'].head())

    for i in range((len(weatherTrain['Data'][0]))):
        weatherPdu = Environment()
        weatherPdu.temperature = weatherTrain['Data'][0][i][0][3] # tempeature
        weatherPdu.pressure = weatherTrain['Data'][0][i][0][4] # pressure
        weatherPdu.humidity = weatherTrain['Data'][0][i][0][5] # humidity 
        weatherPdu.attack = weatherTrain['Data'][0][i][0][6].encode('utf-8')
        weatherPdu.label = weatherTrain['Data'][0][i][0][7]

        memoryStream = BytesIO()
        outputStream = DataOutputStream(memoryStream)
        weatherPdu.serialize(outputStream)
        data = memoryStream.getvalue()

        udpSocket.sendto(data, (DESTINATION_ADDRESS, UDP_PORT))
        print('Temperature: ', weatherTrain['Data'][0][i][0][3])
        print('Pressure: ', weatherTrain['Data'][0][i][0][4])
        print('Humidity: ', weatherTrain['Data'][0][i][0][5])
        print("Sent {}: {}".format(weatherPdu.__class__.__name__, len(data)))
        time.sleep(18)

sendWeatherTrain()