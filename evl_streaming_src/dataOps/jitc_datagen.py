#!/usr/bin/env python 

"""
Application:        JITC processing
File name:          jitc_datagen.py
Author:             Martin Manuel Lopez
Creation:           05/06/2024

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

import os
import json
import pandas as pd
from sklearn.model_selection import train_test_split 
from bitarray import bitarray

class JITC_DATAOPS:
    def __init__(self, dataset):
        self.dataset = dataset
        self.data = []  # List to store processed data
        self.import_data()

    def process_directory(self, directory):
        # print("Processing directory:", directory)
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                filepath = os.path.join(directory, filename)
                self.process_json_file(filepath)

    def process_json_file(self, json_file):
        with open(json_file, 'r') as f:
            json_data = json.load(f)  # Load JSON data
            self.data.append({'filename': os.path.basename(json_file), 'binary': json_data})
        
    def import_data(self):
        self.process_directory(os.getcwd() + '/data/JITC_Data')
        self.data = pd.DataFrame(self.data)

    def develop_dataset(self):
        X = self.data['binary']
        for i in range(len(X)):
            # break down each entry into 8 character bins to create sentence
            X[i] = [X[i][j:j+8] for j in range(0, len(X[i]), 8)]
            print(X[i])
            # convert each 8 character bin into a bitarray
            X[i] = [bitarray(bin_str) for bin_str in X[i]]
            # convert bitarray to decode in ascii
            X[i] = [bit.tobytes().decode('ascii') for bit in X[i]]
            # join all 8 character bins to create a sentence
            X[i] = ''.join(X[i])
            print(X[i])


        X_train, X_test = train_test_split(X, test_size=0.8, random_state=42)

        return X_train, X_test

if __name__ == "__main__":
    dataOps = JITC_DATAOPS(dataset='JITC')
    X_train, X_test= dataOps.develop_dataset()
    print(X_train)
