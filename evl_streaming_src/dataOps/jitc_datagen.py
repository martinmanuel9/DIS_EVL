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
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from tensorflow.keras.preprocessing.sequence import pad_sequences
import tensorflow as tf
from tensorflow.keras.models import Sequential, Model
from tensorflow.keras.layers import LSTM, Dense, Input
from tensorflow.keras.optimizers import Adam

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
                    
    ## --------------------------------------------------------------
    
    def change_directory(self):
        path = os.getcwd()
        ###  debug mode ---------------------------
        # testPath = str(path) + '/data/JITC_Data/'
        # os.chdir(testPath)
        #------------------------------------------
        ### run mode: change path to data directory
        path = Path(path)
        path = path.parents[1]
        changed_path = str(path) + '/data/JITC_Data/'
        os.chdir(changed_path)

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
        self.change_directory()
        self.process_directory(os.getcwd())
        self.dataframe = pd.DataFrame.from_dict(self.data, orient='index', columns=['binary'])
        self.dataframe.index.name = 'filename'

    def develop_dataset(self):
        # Convert binary strings to lists of integers
        self.dataframe['binary'] = self.dataframe['binary'].apply(lambda x: [int(b) for b in x])

        # Pad sequences to the same length
        sequences = pad_sequences(self.dataframe['binary'].tolist(), padding='post')
        
        # Assuming binary labels for the LSTM model (sum of sequence mod 2 as an example)
        labels = np.array([sum(seq) % 2 for seq in sequences])
        
        # Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(sequences, labels, test_size=0.2, random_state=42)
        
        # Reshape for LSTM [samples, time steps, features]
        X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], 1))
        X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], 1))
        
        return X_train, X_test, y_train, y_test

    def build_lstm_model(self, input_shape):
        inputs = Input(shape=input_shape)
        x = LSTM(50, activation='relu', return_sequences=False)(inputs)
        outputs = Dense(1, activation='sigmoid')(x)

        model = Model(inputs, outputs)
        model.compile(optimizer=Adam(), loss='binary_crossentropy', metrics=['accuracy'])
        model.summary()
        feature_extractor = Model(inputs, x)  # Model to extract features from the LSTM layer
        return model, feature_extractor

    def train_and_evaluate_model(self, model, X_train, X_test, y_train, y_test):
        history = model.fit(X_train, y_train, epochs=3, batch_size=32, validation_split=0.2) # used to be 20 epochs
        
        loss, accuracy = model.evaluate(X_test, y_test)
        print(f'Test Accuracy: {accuracy:.2f}')
        return history

if __name__ == "__main__":
    dataOps = JITC_DATAOPS(dataset='JITC')
    # run the following only once to update json files
    # dataOps.update_jsons(os.getcwd() + '/data/JITC_Data')
    
    X_train, X_test, y_train, y_test = dataOps.develop_dataset()
    print(X_train.shape, X_test.shape, y_train.shape, y_test.shape)
    
    lstm_model, feature_extractor = dataOps.build_lstm_model((X_train.shape[1], 1))
    history = dataOps.train_and_evaluate_model(lstm_model, X_train, X_test, y_train, y_test)

    # Extract features from the LSTM layer
    X_train_features = feature_extractor.predict(X_train)
    X_test_features = feature_extractor.predict(X_test)

    # Train a different model (e.g., RandomForest) using the extracted features
    rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_model.fit(X_train_features, y_train)
    y_pred = rf_model.predict(X_test_features)
    rf_accuracy = accuracy_score(y_test, y_pred)
    print(f'Random Forest Test Accuracy: {rf_accuracy:.2f}')


