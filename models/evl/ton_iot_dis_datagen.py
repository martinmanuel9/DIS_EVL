#!/usr/bin/env python 

"""
Application:        Cyber Attacks Data Generation of IoT Devices  
File name:          ton_iot_datagen.py
Author:             Martin Manuel Lopez
Creation:           1/17/2023

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

import pandas as pd
import numpy as np
import os
from pathlib import Path
from sklearn.model_selection import train_test_split  
from category_encoders import OrdinalEncoder 
from datetime import datetime

class TON_IoT_Datagen():
    def __init__(self, dataset):
        self.dataset = dataset
        self.import_data()
        

    def change_directory(self):
        path = str(Path.home())
        path +=  '/DIS_EVL/data/TON_IoT_Data/' 
        # path =  '../data/TON_IoT_Data/'
        os.chdir(path)

    def import_data(self):
        self.change_directory()
        if self.dataset == 'all':
            self.fridge_data()
            self.garage_data()
            self.gps_data()
            self.modbus_data()
            self.light_data()
            self.thermostat_data() 
            self.weather_data()
        elif self.dataset == 'fridge':
            self.fridge_data()
        elif self.dataset == 'garage':
            self.garage_data()
        elif self.dataset == 'gps':
            self.gps_data()
        elif self.dataset == 'modbus':
            self.modbus_data()
        elif self.dataset == 'light':
            self.light_data()
        elif self.dataset == 'thermostat':
            self.thermostat_data()
        elif self.dataset == 'weather':
            self.weather_data()
        else:
            print('Invalid dataset name')

    def fridge_data(self):
        fridge_dataset = pd.read_csv('Train_Test_IoT_Fridge.csv')
        mapping = [{'col': 'temp_condition', 'mapping' : {"low": 1, "high": 2}}, 
                   {'col': 'type', 'mapping': {'normal': 0, 'backdoor': 1, 'ddos': 2, 'injection': 3, 'password': 4, 'ransomware': 5, 'scanning': 6, 'xss': 7 }}] 
        fridgeMapped = OrdinalEncoder(cols=['temp_condition', 'type'], mapping=mapping).fit(fridge_dataset).transform(fridge_dataset)
        complete_fridge_dataset = fridge_dataset[['ts','date','time','fridge_temperature','temp_condition','type','label']]
        complete_fridge_dataset['ts'] = pd.to_numeric(complete_fridge_dataset['ts'])
        complete_fridge_dataset['date'] = pd.to_datetime(complete_fridge_dataset['date'], format="%d-%b-%y")
        complete_fridge_dataset['time'] = complete_fridge_dataset['time'].str.strip()
        complete_fridge_dataset['temp_condition'] = complete_fridge_dataset['temp_condition'].str.strip()
        complete_fridge_dataset['time'] = pd.to_datetime(complete_fridge_dataset['time'], format='%H:%M:%S').dt.time
        complete_fridge_dataset.sort_values(by=['ts','date','time'])
        fridgeMapped = fridgeMapped[['fridge_temperature','temp_condition','type','label']]
        completeTrainFridge,  completeTestFridge = train_test_split(complete_fridge_dataset, test_size=0.33)
        train_fridge, test_fridge = train_test_split(fridgeMapped, test_size=0.33)
        complete_fridge_dataset.sort_values(by=['date']) 
        # print('fridge:', len(train_fridge), len(test_fridge))
        self.fridgeTrainStepsize = 197
        self.fridgeTestStepsize = 396
        self.fridgeTrainSet = train_fridge
        self.fridgeTestSet = test_fridge
        self.completeFridgeTrainSet = completeTrainFridge
        self.completeFridgeTestSet = completeTestFridge

    def garage_data(self):
        garage_dataset = pd.read_csv('Train_Test_IoT_Garage_Door.csv')
        garage_dataset['sphone_signal'] = garage_dataset['sphone_signal'].astype("string")
        mapping = [{'col': 'door_state', 'mapping': {'closed': 0, 'open': 1}}, {'col': 'type', 'mapping': {'normal': 0, 'backdoor': 1, 'ddos': 2, 
                    'injection': 3, 'password': 4, 'ransomeware': 5, 'scanning': 6, 'xss': 7}}, {'col': 'sphone_signal', 'mapping': {'false  ': 0, 'true  ': 1, '0': 0, '1':1}}]
        garageMapped = OrdinalEncoder(cols=['door_state', 'type', 'sphone_signal'], mapping=mapping).fit(garage_dataset).transform(garage_dataset)
        garageMapped = garageMapped[['door_state','sphone_signal','type', 'label']]
        completeMapped = [{'col': 'sphone_signal', 'mapping': {'false  ': 0, 'true  ': 1, '0': 0, '1':1}}]
        complete_garage_dataset = OrdinalEncoder(cols=['sphone_signal'], mapping=completeMapped).fit(garage_dataset).transform(garage_dataset)
        complete_garage_dataset = complete_garage_dataset[['ts','date','time','door_state','sphone_signal','type', 'label']]
        complete_garage_dataset['ts'] = pd.to_numeric(complete_garage_dataset['ts'])
        complete_garage_dataset['date'] = pd.to_datetime(complete_garage_dataset['date'], format="%d-%b-%y")
        complete_garage_dataset['time'] = complete_garage_dataset['time'].str.strip()
        complete_garage_dataset['door_state'] = complete_garage_dataset['door_state'].str.strip()
        complete_garage_dataset['time'] = pd.to_datetime(complete_garage_dataset['time'], format='%H:%M:%S').dt.time
        complete_garage_dataset.sort_values(by=['ts','date','time'])
        train_garage, test_garage = train_test_split(garageMapped, test_size=0.33)
        completeTrainGarage, completeTestGarage = train_test_split(complete_garage_dataset, test_size=0.33)
        # print('garage:', len(train_garage), len(test_garage))
        complete_garage_dataset.sort_values(by=['date'])
        self.garageTrainStepsize = 196
        self.garageTestStepsize = 396
        self.garageTrainSet = train_garage
        self.garageTestSet = test_garage
        self.completeGarageTrainSet = completeTrainGarage
        self.completeGarageTestSet = completeTestGarage
        
    def gps_data(self):
        gps_dataset = pd.read_csv('Train_Test_IoT_GPS_Tracker.csv')
        mapping = [{'col': 'type', 'mapping': {'normal': 0, 'backdoor': 1, 'ddos': 2, 'injection': 3, 'password': 4, 'ransomeware': 5, 'scanning': 6, 'xss': 7}}]
        gpsMapped= OrdinalEncoder(cols=['type'], mapping=mapping).fit(gps_dataset).transform(gps_dataset)
        gpsMapped = gps_dataset[['latitude','longitude','type', 'label']]
        complete_gps_dataset = gps_dataset[['ts','date','time','latitude','longitude','type', 'label']] 
        complete_gps_dataset['ts'] = pd.to_numeric(complete_gps_dataset['ts'])
        complete_gps_dataset['date'] = pd.to_datetime(complete_gps_dataset['date'], format="%d-%b-%y")
        complete_gps_dataset['time'] = complete_gps_dataset['time'].str.strip()
        complete_gps_dataset['time'] = pd.to_datetime(complete_gps_dataset['time'], format='%H:%M:%S').dt.time
        complete_gps_dataset.sort_values(by=['ts','date','time'])
        train_gps, test_gps = train_test_split(gpsMapped, test_size=0.33)
        completeTrainGPS, completeTestGPS = train_test_split(complete_gps_dataset, test_size=0.33)
        # print('gps:', len(train_gps), len(test_gps))
        complete_gps_dataset.sort_values(by=['date'])
        self.gpsTrainStepsize = 194
        self.gpsTestStepsize = 396
        self.gpsTrainSet = train_gps
        self.gpsTestSet = test_gps 
        self.completeGPSTrainSet = completeTrainGPS
        self.completeGPSTestSet = completeTestGPS

    def modbus_data(self):
        modbus_dataset = pd.read_csv('Train_Test_IoT_Modbus.csv')
        mapping = [{'col': 'type', 'mapping': {'normal': 0, 'backdoor': 1, 'injection': 3, 'password': 4, 'scanning': 6, 'xss': 7}}]
        modbusMapped = OrdinalEncoder(cols=['type'], mapping=mapping).fit(modbus_dataset).transform(modbus_dataset)
        features  = ['FC1_Read_Input_Register','FC2_Read_Discrete_Value','FC3_Read_Holding_Register','FC4_Read_Coil','type','label']
        modbusMapped = modbusMapped[features]
        complete_modbus_dataset = modbus_dataset[['ts','date','time','FC1_Read_Input_Register','FC2_Read_Discrete_Value','FC3_Read_Holding_Register','FC4_Read_Coil','type','label']]
        complete_modbus_dataset['ts'] = pd.to_numeric(complete_modbus_dataset['ts'])
        complete_modbus_dataset['date'] = pd.to_datetime(complete_modbus_dataset['date'], format="%d-%b-%y")
        complete_modbus_dataset['time'] = complete_modbus_dataset['time'].str.strip()
        complete_modbus_dataset['time'] = pd.to_datetime(complete_modbus_dataset['time'], format='%H:%M:%S').dt.time
        completeTrainModbus, completeTestModbus = train_test_split(complete_modbus_dataset, test_size=0.33)
        train_modbus, test_modbus = train_test_split(modbusMapped, test_size=0.33)
        # train_modbus = train_modbus[features]
        # test_modbus = test_modbus[features]
        # print('modbus:', len(train_modbus), len(test_modbus))
        complete_modbus_dataset.sort_values(by=['date'])
        self.modbusTrainStepsize = 168
        self.modbusTestStepsize = 336
        self.modbusTrainSet = train_modbus
        self.modbusTestSet = test_modbus
        self.completeModbusTrainSet = completeTrainModbus
        self.completeModbusTestSet = completeTestModbus
    
    def light_data(self):
        light_dataset = pd.read_csv('Train_Test_IoT_Motion_Light.csv')
        mapping = [{'col':'light_status', 'mapping': {' off': 0, ' on': 1}}, {'col': 'type', 'mapping':{'normal': 0, 'backdoor': 1, 'ddos': 2, 'injection': 3, 'password': 4, 'ransomeware': 5, 'scanning': 6, 'xss': 7 }}]
        lightMapped = OrdinalEncoder(cols=['light_status', 'type'], mapping = mapping).fit(light_dataset).transform(light_dataset)
        lightMapped = lightMapped[['motion_status','light_status','type','label']]
        complete_light_dataset = light_dataset[['ts','date','time','motion_status','light_status','type','label']]
        complete_light_dataset['ts'] = pd.to_numeric(complete_light_dataset['ts'])
        complete_light_dataset['date'] = pd.to_datetime(complete_light_dataset['date'], format="%d-%b-%y")
        complete_light_dataset['time'] = complete_light_dataset['time'].str.strip()
        complete_light_dataset['time'] = pd.to_datetime(complete_light_dataset['time'], format='%H:%M:%S').dt.time
        complete_light_dataset['light_status'] = complete_light_dataset['light_status'].str.strip()
        train_light, test_light = train_test_split(lightMapped, test_size=0.33)
        completeTrainLight, completeTestLight = train_test_split(complete_light_dataset, test_size=0.33) 
        complete_light_dataset.sort_values(by=['date'])
        self.lightTrainStepsize = 196
        self.lightTestStepsize = 396
        self.lightTrainSet = train_light
        self.lightTestSet = test_light
        self.completeLightTrainSet = completeTrainLight
        self.completeLightTestSet = completeTestLight

    def thermostat_data(self):
        thermostat_dataset = pd.read_csv('Train_Test_IoT_Thermostat.csv')
        mapping = [{'col': 'type', 'mapping':{'normal': 0, 'backdoor': 1, 'injection': 3, 'password': 4, 'ransomeware': 5, 'scanning': 6, 'xss': 7 }}]
        thermostatMapped = OrdinalEncoder(cols=['type'], mapping=mapping).fit(thermostat_dataset).transform(thermostat_dataset)
        thermostatMapped = thermostatMapped[['current_temperature','thermostat_status','type','label']]
        complete_thermostat_dataset = thermostat_dataset[['ts','date','time','current_temperature','thermostat_status','type','label']]
        complete_thermostat_dataset['ts'] = pd.to_numeric(complete_thermostat_dataset['ts'])
        complete_thermostat_dataset['date'] = pd.to_datetime(complete_thermostat_dataset['date'], format="%d-%b-%y")
        complete_thermostat_dataset['time'] = complete_thermostat_dataset['time'].str.strip()
        complete_thermostat_dataset['time'] = pd.to_datetime(complete_thermostat_dataset['time'], format='%H:%M:%S').dt.time
        train_thermo, test_thermo = train_test_split(thermostat_dataset, test_size=0.33)
        completeTrainThermo, completeTestThermo = train_test_split(complete_thermostat_dataset, test_size=0.33) 
        completeTestThermo.sort_values(by=['date'])
        # print('thermo', len(train_thermo), len(test_thermo))
        self.thermoTrainStepsize = 174
        self.thermoTestStepsize = 348
        self.thermoTrainSet = train_thermo
        self.thermoTestSet = test_thermo
        self.completeThermoTrainSet = completeTrainThermo
        self.completeThermoTestSet = completeTestThermo

    def weather_data(self):
        weather_dataset = pd.read_csv('Train_Test_IoT_Weather.csv')
        mapping = [{'col': 'type', 'mapping':{'normal': 0, 'backdoor': 1, 'ddos': 2, 'injection': 3, 'password': 4, 'ransomeware': 5, 'scanning': 6, 'xss': 7 }}]
        weatherMapped = OrdinalEncoder(cols=['type'], mapping=mapping).fit(weather_dataset).transform(weather_dataset)
        weatherMapped = weatherMapped[['temperature','pressure','humidity','type','label']]
        complete_weather_dataset = weather_dataset[['ts','date','time','temperature','pressure','humidity','type','label']]
        complete_weather_dataset['ts'] = pd.to_numeric(complete_weather_dataset['ts'])
        complete_weather_dataset['date'] = pd.to_datetime(complete_weather_dataset['date'], format="%d-%b-%y")
        complete_weather_dataset['time'] = complete_weather_dataset['time'].str.strip()
        complete_weather_dataset['time'] = pd.to_datetime(complete_weather_dataset['time'], format='%H:%M:%S').dt.time
        complete_weather_dataset['label'] = pd.to_numeric(complete_weather_dataset['label']) 
        train_weather, test_weather = train_test_split(weatherMapped, test_size=0.33)
        completeTrainWeather, completeTestWeather = train_test_split(complete_weather_dataset, test_size=0.33) 
        # print('weather:', len(train_weather), len(test_weather))
        complete_weather_dataset.sort_values(by=['date'])
        self.weatherTrainStepsize = 194
        self.weatherTestStepsize = 396
        self.weatherTrainSet = train_weather
        self.weatherTestSet = test_weather
        self.completeWeatherTrainSet = completeTrainWeather
        self.completeWeatherTestSet = completeTestWeather
        
    
    def batch(self, iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield np.array(iterable[ndx:min(ndx + n, l)])

    def create_dataset(self, train_stepsize, test_stepsize, test, train): 
        self.trainDict = {}
        self.testDict = {}
        trainSet = test.to_numpy()
        testSet = train.to_numpy()
        column_names = test.columns

        a = []
        indx = []
        for d in range(train_stepsize-1): # teststpesize
            a.append(d)

        indx = a

        self.trainDataset = trainSet
        self.trainData = trainSet
        self.trainLabels = trainSet[:,-1]
        self.trainUse = trainSet[:train_stepsize]
        self.trainUse[:,-1][indx] = 1

        self.testDataset = testSet
        self.testData = testSet
        self.testLabels = testSet[:,-1]
        self.testUse = testSet[:test_stepsize]
        self.testUse[:,-1][indx] = 1

        trainDataset = []
        X_train = []
        for i in self.batch(self.trainData, train_stepsize):
            trainDataset.append(i)
        X_train.append(trainDataset)
        self.trainData = np.array(X_train, dtype=object)

        testDataset = []
        y_test = []
        for i in self.batch(self.testData, test_stepsize):
            testDataset.append(i)
        y_test.append(testDataset)
        self.testData = np.array(y_test, dtype=object)

        trainLabels = []
        lblTrainData = []
        for i in self.batch(self.trainLabels, train_stepsize):
            trainLabels.append(i)
        lblTrainData.append(trainLabels)
        self.trainLabels = lblTrainData

        testLabels = []
        lblTestData = []
        for i in self.batch(self.testLabels, test_stepsize):
            testLabels.append(i)
        lblTestData.append(trainLabels)
        self.testLabels = lblTestData

        self.trainDict['Dataset'] = self.trainDataset
        self.trainDict['Data'] = self.trainData
        self.trainDict['Labels'] = self.trainLabels
        self.trainDict['Use'] = self.trainUse
        self.trainDict['Dataframe'] = pd.DataFrame(self.trainDataset, columns=column_names)

        self.testDict['Dataset'] = self.testDataset
        self.testDict['Data'] = self.testData
        self.testDict['Labels'] = self.testLabels
        self.testDict['Use'] = self.testUse
        self.testDict['Dataframe'] = pd.DataFrame(self.testDataset, columns=column_names)

        return self.trainDict, self.testDict

# datagen = TON_IoT_Datagen(dataset='fridge')
# fridgeTrain, fridgeTest = datagen.create_dataset(train_stepsize=datagen.fridgeTrainStepsize, test_stepsize=datagen.fridgeTestStepsize, 
#                                 train= datagen.completeFridgeTestSet, test = datagen.completeFridgeTestSet)
# print(fridgeTrain['Data'])