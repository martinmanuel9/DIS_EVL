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

from evl_models import ton_iot_datagen as ton
import numpy as np

# Create garage dataset and timesteps for simulation
gpsDataset = ton.TON_IoT_Datagen()
gpsTrain, gpsTest = gpsDataset.create_dataset(train_stepsize=gpsDataset.gpsTrainStepsize, test_stepsize=gpsDataset.gpsTestStepsize, 
                                train= gpsDataset.gpsTrainSet, test = gpsDataset.gpsTestSet)

# print(np.shape(gpsTrain['Data']), np.shape(gpsTest['Data']))