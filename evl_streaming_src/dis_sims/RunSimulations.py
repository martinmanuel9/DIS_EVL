#!/usr/bin/env python 

"""
Application:        Run IoT Simulations 
File name:          RunSimulations.py
Author:             Martin Manuel Lopez
Creation:           9/19/2023

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

import argparse
import threading
import devices.fridgeSim as fridgeSim
import devices.garageSim as garageSim
import devices.gpsSim as gpsSim
import devices.lightSim as lightSim
import devices.modbusSim as modbusSim
import devices.weatherSim as weatherSim
import devices.thermostatSim as thermostatSim
class DeviceTrain():
    def __init__(self, transmission, speed):
        self.transmission = transmission
        self.speed = speed

    def runTrainingData(self):
        try:
            threads = []

            fridgeTrain = fridgeSim.FridgeSim(transmission=self.transmission, speed = self.speed)
            garageTrain = garageSim.GarageSim(transmission=self.transmission, speed = self.speed)
            gpsTrain = gpsSim.GPSSim(transmission=self.transmission, speed= self.speed)
            lightTrain = lightSim.LightSim(transmission=self.transmission, speed= self.speed)
            modbusTrain = modbusSim.ModbusSim(transmission=self.transmission, speed= self.speed)
            weatherTrain = weatherSim.WeatherSim(transmission=self.transmission, speed= self.speed)
            thermnostatTrain = thermostatSim.ThermostatSim(transmission=self.transmission, speed= self.speed)

            # Create threads for each simulation and add them to the list
            threads.append(threading.Thread(target=fridgeTrain.sendFridgeTrain))
            threads.append(threading.Thread(target=garageTrain.sendGarageTrain))
            threads.append(threading.Thread(target=gpsTrain.sendGPSTrain))
            threads.append(threading.Thread(target=lightTrain.sendLightTrain))
            threads.append(threading.Thread(target=modbusTrain.sendModbusTrain))
            threads.append(threading.Thread(target=thermnostatTrain.sendThermostatTrain))
            threads.append(threading.Thread(target=weatherTrain.sendWeatherTrain))
            

            # Start all threads
            for thread in threads:
                thread.start()

            # Wait for all threads to finish
            for thread in threads:
                thread.join()

        except Exception as e:
            print(f"Error: {e}")
            print("Error: Could not run training data.")
            return False
        
    def runTestData(self):
        try:
            threads = []
            fridgeTest = fridgeSim.FridgeSim(transmission=self.transmission, speed= self.speed)
            garageTest = garageSim.GarageSim(transmission=self.transmission, speed= self.speed)
            gpsTest = gpsSim.GPSSim(transmission=self.transmission, speed= self.speed)
            lightTest = lightSim.LightSim(transmission=self.transmission, speed= self.speed)
            modbusTest = modbusSim.ModbusSim(transmission=self.transmission, speed= self.speed)
            weatherTest = weatherSim.WeatherSim(transmission=self.transmission, speed= self.speed)
            thermostatTest = thermostatSim.ThermostatSim(transmission=self.transmission, speed= self.speed)

            # Create threads for each simulation and add them to the list
            threads.append(threading.Thread(target=fridgeTest.sendFridgeTest))
            threads.append(threading.Thread(target=garageTest.sendGarageTest))
            threads.append(threading.Thread(target=gpsTest.sendGPSTest))
            threads.append(threading.Thread(target=lightTest.sendLightTest))
            threads.append(threading.Thread(target=modbusTest.sendModbusTest))
            threads.append(threading.Thread(target=thermostatTest.sendThermostatTest))
            threads.append(threading.Thread(target=weatherTest.sendWeatherTest))
            

            # Start all threads
            for thread in threads:
                thread.start()

            # Wait for all threads to finish
            for thread in threads:
                thread.join()

        except Exception as e:
            print(f"Error: {e}")
            print("Error: Could not run training data.")
            return False
        
def main():
    parser = argparse.ArgumentParser(description="Run Devices")
    parser.add_argument("--transmission", choices=["pdu", "kafka", "kafka_pdu"], default="kafka_pdu", help="Transmission option")
    parser.add_argument("--mode", choices=["train", "test"], default="train", help="Mode: train or test")
    parser.add_argument("--speed", choices=["fast", "slow"], default="fast", help="Speed: fast or slow")

    args = parser.parse_args()

    deviceTrain = DeviceTrain(transmission=args.transmission, speed = args.speed)

    if args.mode == "train":
        deviceTrain.runTrainingData()
    elif args.mode == "test":
        deviceTrain.runTestData()
    else:
        print("Invalid mode. Please specify 'train' or 'test'.")

if __name__ == "__main__":
    main()

