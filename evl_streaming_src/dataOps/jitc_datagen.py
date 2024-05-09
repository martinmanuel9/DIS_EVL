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
        self.data = {}  # dict to store processed data
        self.dataframe = pd.DataFrame()
        self.import_data()


    ## Step 1: Get all json files from onedrive and place them in a single directory 
    ## Step 2: Run this once once you download all files and put them in a single directory 
    def update_jsons(self, directory):
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                #prepend and append {} of each file 
                with open(directory + '/' + filename, 'r') as f:
                    data = f.read()
                    data = '{\"binary\":' + data + '}'
                with open(directory + '/' + filename, 'w') as f:
                    f.write(data)

    # add binaries into a list
    def process_directory(self, directory):
        # print("Processing directory:", directory)
        # need to prepend each file with {} to make json file correct
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                filepath = os.path.join(directory, filename)
                self.process_json_file(filepath)

    def process_json_file(self, json_file):
        with open(json_file, 'r') as f:
            json_data = json.load(f)  # Load JSON data
            # print(json_data['binary'])
            self.data[os.path.basename(json_file)] = json_data['binary']
        
    def import_data(self):
        self.process_directory(os.getcwd() + '/data/JITC_Data')
        self.dataframe = pd.DataFrame.from_dict(self.data, orient='index', columns=['binary'])
        self.dataframe.index.name = 'filename'
        
    def decode_dataset(self, data):
        X = data
        # go through dataframe and decode binary data
        for i in range(len(X)):
            # print('Before:\n', type(X['binary'].iloc[i]))
            binary_str = X['binary'].iloc[i]
            # convert to bytes
            bytes_list = [binary_str[i:i+8] for i in range(0, len(binary_str), 8)]
            byte_values = [int(byte, 2) for byte in bytes_list]
            decoded_values = [bytes([byte]).decode('utf-8') for byte in byte_values]
            print('Decoded:', decoded_values)
        return X
    

    def develop_dataset(self, ):
        X_train, X_test = train_test_split(self.dataframe, test_size=0.8, random_state=42)
        
        X_train_decoded = self.decode_dataset(X_train)
        
        
        # X_train = pd.DataFrame(X_train)
        # # Find duplicate rows based on the 'binary' column
        # duplicates = X_train.duplicated(subset=['binary'], keep=False)
        
        # X_train['label'] = ~duplicates

        # # Print instances where label is True
        # print(X_train[X_train['label'] == True])
        
        


        return X_train, X_test

if __name__ == "__main__":
    dataOps = JITC_DATAOPS(dataset='JITC')
    # run the following only once to update json files
    # dataOps.update_jsons(os.getcwd() + '/data/JITC_Data')
    X_train, X_test= dataOps.develop_dataset()

