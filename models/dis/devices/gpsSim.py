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

# Create garage dataset and timesteps for simulation
gpsDataset = ton.TON_IoT_Datagen()
gpsTrain, gpsTest = gpsDataset.create_dataset(train_stepsize=gpsDataset.gpsTrainStepsize, test_stepsize=gpsDataset.gpsTestStepsize, 
                                train= gpsDataset.completeGPSTrainSet, test = gpsDataset.completeGPSTestSet)

# try to send the first timestep to using the opendis 