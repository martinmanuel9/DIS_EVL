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

UDP_PORT = 3001
DESTINATION_ADDRESS = "127.0.0.1"

udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)


# Create garage dataset and timesteps for simulation
gpsDataset = ton.TON_IoT_Datagen()
gpsTrain, gpsTest = gpsDataset.create_dataset(train_stepsize=gpsDataset.gpsTrainStepsize, test_stepsize=gpsDataset.gpsTestStepsize, 
                                train= gpsDataset.completeGPSTrainSet, test = gpsDataset.completeGPSTestSet)

gps = GPS()

def sendTrainGPS():
    columnNames = gpsTrain['Dataframe'].columns
    # print(gpsTrain['Dataframe'].head())
    for i in range(len(gpsTrain['Data'][0])):
        gpsPDU = EntityStatePdu()
        gpsPDU.entityID.entityID = 42
        gpsPDU.entityID.siteID = 17
        gpsPDU.entityID.applicationID = 23
        gpsPDU.marking.setString('Igor3d')

        gpsLocation = gps.llarpy2ecef(np.deg2rad(gpsTrain['Data'][0][i][0][3]),   # longitude (radians)   
                                    np.deg2rad(gpsTrain['Data'][0][i][0][4]), # latitude (radians)
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

        gpsPDU.attack = gpsTrain['Data'][0][i][0][5].encode()
        gpsPDU.label = gpsTrain['Data'][0][i][0][6] 

        memoryStream = BytesIO()
        outputStream = DataOutputStream(memoryStream)
        gpsPDU.serialize(outputStream)
        data = memoryStream.getvalue()

        udpSocket.sendto(data, (DESTINATION_ADDRESS, UDP_PORT))
        print('Train Data GPS: \n', 'Longitude: ', gpsTrain['Data'][0][i][0][3], '\n', 'Latitude:', gpsTrain['Data'][0][i][0][4]) # latitude, longitude
        print('Packet Size: ' , sys.getsizeof(gpsTrain['Data'][0][i][0][3]))
        print("Sent {}: {} bytes".format(gpsPDU.__class__.__name__, len(data)))
        time.sleep(10)

sendTrainGPS()
