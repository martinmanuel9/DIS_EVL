#!/usr/bin/env python 

"""
Application:        DIS Simulation of Receiver 
File name:          DISReceiver.py
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

import socket 
import time
import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from opendismodel.opendis.dis7 import *
from opendismodel.opendis.PduFactory import createPdu
from opendismodel.opendis.RangeCoordinates import *

UDP_PORT = 3001

udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udpSocket.bind(("", UDP_PORT))

print("Listening for DIS on UDP socket {}".format(UDP_PORT))
gps = GPS()

def recv():
    data = udpSocket.recv(1024) # buffer size in bytes
    pdu = createPdu(data)
    pduTypeName = pdu.__class__.__name__
     
    if pdu.pduType == 1: # PduTypeDecoders.EntityStatePdu:
        loc = (pdu.entityLocation.x,
                pdu.entityLocation.y,
                pdu.entityLocation.z,
                pdu.entityOrientation.psi,
                pdu.entityOrientation.theta,
                pdu.entityOrientation.phi
                )
        
        body = gps.ecef2llarpy(*loc)
        print("Received {}: {} Bytes\n".format(pduTypeName, len(data), flush=True)
                + " Id        : {}\n".format(pdu.entityID.entityID)
                + " Latitude  : {:.2f} degrees\n".format(rad2deg(body[0]))
                + " Longitude : {:.2f} degrees\n".format(rad2deg(body[1]))
                + " Altitude  : {:.0f} meters\n".format(body[2])
                + " Yaw       : {:.2f} degrees\n".format(rad2deg(body[3]))
                + " Pitch     : {:.2f} degrees\n".format(rad2deg(body[4]))
                + " Roll      : {:.2f} degrees\n".format(rad2deg(body[5]))
                + " Attack    : {}\n".format(pdu.attack.decode('utf-8'))
                + " Label     : {}\n".format(pdu.label)
                )
    
    elif pdu.pduType == 73: # Light
        print("Received {}: {} Bytes\n".format(pduTypeName, len(data), flush=True)
              + " Motion Status    : {}\n".format(pdu.motion_status)
              + " Light Status     : {}\n".format(pdu.light_status.decode('utf-8'))
              + " Attack           : {}\n".format(pdu.attack.decode('utf-8'))
              + " Label            : {}\n".format(pdu.label)
              )
    
    elif pdu.pduType == 70:  # environment
        print("Received {}: {} Bytes \n".format(pduTypeName, len(data), flush=True)
                + " Device: {}\n".format(pdu.device.decode('utf-8'))
                + " Temperature: {}\n".format(pdu.temperature)
                + " Pressure: {}\n".format(pdu.pressure)
                + " Humidity: {}\n".format(pdu.humidity)
                + " Condition: {}\n".format(pdu.condition.decode('utf-8'))
                + " Temp Status: {}\n".format(pdu.temp_status)
                + " Attack: {}\n".format(pdu.attack.decode('utf-8'))
                + " Label: {}\n".format(pdu.label)  
                )
        
    elif pdu.pduType == 71: # modbus
        print("Received {}: {} Bytes\n".format(pduTypeName, len(data), flush=True)
            + " FC1 Read Input Register: {}\n".format(pdu.fc1)
            + " FC2 Read Discrete Value: {}\n".format(pdu.fc2)
            + " FC3 Read Holding Register: {}\n".format(pdu.fc3)
            + " FC4 Read Coil: {}\n".format(pdu.fc4)
            + " Attack: {}\n".format(pdu.attack.decode('utf-8'))
            + " Label : {}\n".format(pdu.label)
            )
    
    elif pdu.pduType == 72: # garage
        print("Received {}: {} Bytes\n".format(pduTypeName, len(data), flush=True)
            + " Door State: {}\n".format(pdu.door_state.decode('utf-8'))
            + " SPhone: {}\n".format(pdu.sphone)
            + " Attack: {}\n".format(pdu.attack.decode('utf-8'))
            + " Label : {}\n".format(pdu.label)
            )

    else: 
        print("Received not a PDU?? {}, {} bytes".format(pduTypeName, len(data)), flush=True)

while True:
    recv()