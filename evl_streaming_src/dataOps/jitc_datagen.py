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
from tensorflow.keras.optimizers import Adam
from sklearn.mixture import GaussianMixture
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor
from sklearn.decomposition import PCA

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
        testPath = str(path) + '/data/JITC_Data/files/'
        os.chdir(testPath)
        #------------------------------------------
        ### run mode: change path to data directory
        # print(path)
        # changed_path = path + '/JITC_Data/files'
        # # os.chdir(changed_path)
        # print(os.getcwd())

    def process_directory(self, directory):
        with ThreadPoolExecutor() as executor:
            json_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith('.json')]
            executor.map(self.process_json_file, json_files)

    def process_json_file(self, json_file):
        with open(json_file, 'r') as f:
            json_data = json.load(f)  # Load JSON data
            self.data[os.path.basename(json_file)] = json_data['binary']

    def import_data(self):
        self.change_directory()
        self.process_directory(os.getcwd())
        self.dataframe = pd.DataFrame.from_dict(self.data, orient='index', columns=['binary'])
        self.dataframe.index.name = 'filename'
        
        # for each key determine how many bytes are in the binary string I need to break it down 8 bits per byte
        self.dataframe['num_bytes'] = self.dataframe['binary'].apply(lambda x: len(x) // 8)
        
        print(self.dataframe.head())
        
        # find the average num_bytes in all of the files within the dataframe
        avg_bytes = self.dataframe['num_bytes'].mean()
        print('Average number of bytes: ', avg_bytes)
        median_bytes = self.dataframe['num_bytes'].median()
        print('Median number of bytes: ', median_bytes)
        
        # save the dataframe to as a pickle file on evl_streaming_src/datasets with the name of the dataset and date
        os.chdir('../dataframe/')
        # get date as YYYYMMDD_HHMMSS
        date = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        self.dataframe.to_pickle('JITC_Dataframe_'+ date + '.pkl')
        
        os.chdir('../files')

    # def find_silhoette_score(self, X):
    #     """
    #     Find Silhoette Scores allows us to get the optimal number of clusters for the data
    #     """
    #     X = np.array(X)
    #     X = X.astype(int)
    #     X = X.reshape(-1, 1)

    #     sil_score = {}
    #     for c in range(2, 30):

    #         kmeans_model = KMeans(n_clusters=c, n_init='auto').fit(X)
    #         score = silhouette_score(X, kmeans_model.labels_, metric='euclidean')
    #         sil_score[c] = score
    #     optimal_cluster = max(sil_score, key=sil_score.get)
    #     return optimal_cluster

    # def convert_binary_string(self, binary_string):
    #     return [int(b) for b in binary_string]

    def develop_dataset(self):
        # # Convert binary strings to lists of integers using parallel processing
        # with ThreadPoolExecutor() as executor:
        #     self.dataframe['binary'] = list(executor.map(self.convert_binary_string, self.dataframe['binary']))

        # Flatten the array of arrays into a single list of bits
        flat_data = [bit for array in self.dataframe['binary'] for bit in array]

        # Group the bits into chunks of 8
        bytes_list = [flat_data[i:i + 8] for i in range(0, len(flat_data), 8)]
        bytes_list = np.array(bytes_list)
        bytes_list = bytes_list.astype(int)

        print(np.shape(bytes_list))

        # nClusters = self.find_silhoette_score(bytes_list)
        nClusters = 30

        # GMM clustering
        GMMCluster = GaussianMixture(n_components=nClusters, random_state=42).fit(bytes_list)
        gmm_labels = GMMCluster.predict(bytes_list)

        # KMeans clustering
        KMeanCluster = KMeans(n_clusters=nClusters, random_state=42).fit(bytes_list)
        kmeans_labels = KMeanCluster.labels_

        # Dimensionality reduction for visualization
        pca = PCA(n_components=2)
        bytes_list_2D = pca.fit_transform(bytes_list)

        # Graph GMM Cluster
        plt.scatter(bytes_list_2D[:, 0], bytes_list_2D[:, 1], c=gmm_labels, s=40, cmap='viridis')
        plt.title('GMM Clustering')
        plt.show()
        plt.savefig('GMMCluster.png')

        # Graph KMeans Cluster
        plt.scatter(bytes_list_2D[:, 0], bytes_list_2D[:, 1], c=kmeans_labels, s=40, cmap='viridis')
        plt.title('KMeans Clustering')
        plt.show()
        plt.savefig('KmeansCluster.png')


        # # show figure using plotly graph objects based on on the plt
        # fig = go.Figure(data=[go.Scatter(x=bytes_list[:, 0], y=bytes_list[:, 1], mode='markers', marker=dict(color=KMeanCluster.predict(bytes_list)))])
        # fig.show()

        # split data into train and test
        X_train, X_test = train_test_split(bytes_list, test_size=0.8, random_state=42)

        # create classes & labels based on the clusters identified
        y_train = GMMCluster.predict(X_train)
        y_test = GMMCluster.predict(X_test)


        return X_train, X_test, y_train, y_test


if __name__ == "__main__":
    dataOps = JITC_DATAOPS(dataset='JITC')
    # run the following only once to update json files
    # dataOps.update_jsons(os.getcwd() + '/data/JITC_Data')

    X_train, X_test, y_train, y_test = dataOps.develop_dataset()

    # make new directory called artifacts and change to that directory
    # check if there is a directory called artifacts
    if not os.path.exists('artifacts'):
        os.mkdir('artifacts')
        os.chdir('artifacts')
    else:
        os.chdir('artifacts')

    # save X_train, X_test, y_train, y_test in pickle and h5 format
    X_train = pd.DataFrame(X_train)
    X_test = pd.DataFrame(X_test)
    y_train = pd.DataFrame(y_train)
    y_test = pd.DataFrame(y_test)
    X_train.to_pickle('X_train.pkl')
    X_test.to_pickle('X_test.pkl')
    y_train.to_pickle('y_train.pkl')
    y_test.to_pickle('y_test.pkl')
