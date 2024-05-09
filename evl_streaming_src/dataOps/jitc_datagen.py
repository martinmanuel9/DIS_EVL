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
import numpy as np
from sklearn.model_selection import train_test_split 
from bitarray import bitarray
import binascii
import html
import base64


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
            # remove quotes 
            json_data = json_data.replace("\"", "")
            self.data.append({'filename': os.path.basename(json_file), 'binary': json_data})
        
    def import_data(self):
        self.process_directory(os.getcwd() + '/data/JITC_Data')
        self.data = pd.DataFrame(self.data)
        
    def decode_dataset(self, data):
        X = data.to_frame()
        # go through dataframe and decode binary data
        for i in range(len(X)):
            print('Before:\n',X['binary'].iloc[i][0])
            X['binary'].iloc[i] = binascii.b2a_base64(bitarray(X['binary'].iloc[i]), newline=True).decode()
            decodedBytes = base64.b64decode(X['binary'].iloc[i])
            X['binary'].iloc[i] = decodedBytes.decode('ascii')
            
        
        print('After:\n', X)
        return X
    

    def develop_dataset(self, ):
        X_train, X_test = train_test_split(self.data['binary'], test_size=0.8, random_state=42)
        
        # X_train_decoded = self.decode_dataset(X_train)

        return X_train, X_test

# if __name__ == "__main__":
#     dataOps = JITC_DATAOPS(dataset='JITC')
#     X_train, X_test= dataOps.develop_dataset()
#     print(X_train)
