#!/usr/bin/env python 

"""
Application:        Run Devices 
File name:          RunDevices.py
Author:             Martin Manuel Lopez
Creation:           9/19/2023

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

# import subprocess
# import concurrent.futures

# class DeviceTrain():
#     def runTrainingData(self):
#         try:
#             with concurrent.futures.ProcessPoolExecutor() as executor:
#                 # Submit each simulation as a separate task
#                 futures = [
#                     executor.submit(DeviceTrain.run_simulation, 'fridgeSim'),
#                     executor.submit(DeviceTrain.run_simulation, 'garageSim'),
#                     executor.submit(DeviceTrain.run_simulation, 'gpsSim'),
#                     executor.submit(DeviceTrain.run_simulation, 'lightSim'),
#                     executor.submit(DeviceTrain.run_simulation, 'modbusSim'),
#                     executor.submit(DeviceTrain.run_simulation, 'weatherSim')
#                 ]

#                 # Wait for all tasks to complete
#                 concurrent.futures.wait(futures)

#         except Exception as e:
#             print(f"Error: {e}")
#             print("Error: Could not run training data.")
#             return False


#     def run_simulation(self, simulation_name):
#         try:
#             subprocess.run(['python', '-m', f'models.dis.devices.{simulation_name}'], check=True)
#         except subprocess.CalledProcessError as e:
#             print(f"Error running {simulation_name}: {e}")

# if __name__ == "__main__":
#     device_train = DeviceTrain()
#     device_train.runTrainingData()


import subprocess

class DeviceTrain():
    def runTrainingData(self):
        try:
            subprocess.Popen(['python', '-m', 'models.dis.devices.gpsSim']).wait()
            subprocess.Popen(['python', '-m', 'models.dis.devices.fridgeSim']).wait()
            subprocess.Popen(['python', '-m', 'models.dis.devices.garageSim']).wait()
            subprocess.Popen(['python', '-m', 'models.dis.devices.lightSim']).wait()
            subprocess.Popen(['python', '-m', 'models.dis.devices.modbusSim']).wait()
            subprocess.Popen(['python', '-m', 'models.dis.devices.weatherSim']).wait()
        except Exception as e:
            print(f"Error: {e}")
            print("Error: Could not run training data.")
            return False

if __name__ == "__main__":
    device_train = DeviceTrain()
    device_train.runTrainingData(transmission='pdu')

