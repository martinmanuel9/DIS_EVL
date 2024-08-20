#!/usr/bin/env python

"""
Application:        JITC processing
File name:          jitc_datagen_ngrams.py
Author:             Martin Manuel Lopez
Creation:           08/17/2024

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
from collections import Counter
from nltk.util import ngrams
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import DBSCAN
from sklearn.model_selection import train_test_split
import umap
import matplotlib.pyplot as plt

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
        # changed_path = path + '/drive/MyDrive/JITC_Data/files'
        # os.chdir(changed_path)
        # print(os.getcwd())

    def process_directory(self, directory):
        with ThreadPoolExecutor() as executor:
            json_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith('.json')]
            executor.map(self.process_json_file, json_files)

    def process_json_file(self, json_file, n=4):
        with open(json_file, 'r') as f:
            json_data = json.load(f)  # Load JSON data

            # Process binary data for n-grams
            binary_sequence = json_data['binary']
            ngrams = self.get_ngrams(binary_sequence, n)
            ngram_freq = self.get_ngrams_frequency(ngrams)

            # Store the binary data, n-grams, and their frequencies
            self.data[os.path.basename(json_file)] = {
                'binary': binary_sequence,
                'ngrams': ngrams,
                'ngrams_freq': ngram_freq
            }

    def get_ngrams(self, sequence, n):
        """
        Generate n-grams from a given sequence.
        """
        ngrams = [sequence[i:i+n] for i in range(len(sequence) - n + 1)]
        return ngrams

    def get_ngrams_frequency(self, ngrams):
        """
        Count the frequency of each n-gram.
        """
        return dict(Counter(ngrams))

        
    def import_data(self):
        self.change_directory()
        self.process_directory(os.getcwd())
        self.dataframe = pd.DataFrame.from_dict(self.data, orient='index', columns=['binary', 'ngrams', 'ngrams_freq'])
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
        if not os.path.exists('../dataframe'):
            os.mkdir('../dataframe')
        os.chdir('../dataframe/')
        # get date as YYYYMMDD_HHMMSS
        date = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        self.dataframe.to_pickle('JITC_Dataframe_'+ date + '.pkl')

        os.chdir('../files')

    def develop_dataset(self):
        # read the dataframe 
        df_jitc_ngrams = self.dataframe['ngrams_freq']
        # drop filename column
        df_jitc_ngrams = df_jitc_ngrams.drop(columns=['filename'])
        # create new dataframe with columns as the keys of the dictionary
        df_jitc_ngrams = pd.DataFrame(df_jitc_ngrams.tolist(), columns=df_jitc_ngrams.iloc[0].keys())
        print(df_jitc_ngrams.head())
        
        # Step 1: Standardize the data
        scaler = MinMaxScaler()
        X_scaled = scaler.fit_transform(df_jitc_ngrams)
        
        X_train, X_test = train_test_split(X_scaled, test_size=0.8, random_state=42)
        
        # create classes & labels based on the clusters identified
        y_train = DBSCAN(eps=0.5, min_samples=5).fit_predict(X_train)
        y_test = DBSCAN(eps=0.5, min_samples=5).fit_predict(X_test)
        
        # Step 2: Run DBSCAN
        dbscan = DBSCAN(eps=0.1, min_samples=4)  # You may need to tune eps and min_samples
        labels = dbscan.fit_predict(X_scaled)

        # Step 3: Count unique labels (excluding noise)
        unique_labels = set(labels) - {-1}  # Exclude noise points with label -1
        n_clusters = len(unique_labels)

        print(f"Number of clusters: {n_clusters}")
        print(f"Labels: {labels}")

        # Apply UMAP to reduce to 2D
        reducer = umap.UMAP(n_components=2, random_state=42)
        X_umap = reducer.fit_transform(X_scaled)
        
        # Plotting the UMAP results
        plt.figure(figsize=(8, 6))
        unique_labels = set(labels)

        # Colors for the plot
        colors = [plt.cm.Spectral(each) for each in np.linspace(0, 1, len(unique_labels))]

        for k, col in zip(unique_labels, colors):
            if k == -1:
                # Black used for noise (points labeled as -1)
                col = [0, 0, 0, 1]

            class_member_mask = (labels == k)

            plt.plot(X_umap[class_member_mask, 0], X_umap[class_member_mask, 1], 'o',
                    markerfacecolor=tuple(col),
                    markeredgecolor='k',
                    markersize=6)

        plt.title('DBSCAN Clusters Visualized using UMAP')
        plt.xlabel('UMAP Component 1')
        plt.ylabel('UMAP Component 2')
        plt.savefig('DBSCAN_Clusters_Visualized_using_UMAP_2D.png')
        plt.show()
        
        # Apply UMAP to reduce to 3D
        reducer = umap.UMAP(n_components=3, random_state=42)
        X_umap_3d = reducer.fit_transform(X_scaled)

        # 3D Plotting
        from mpl_toolkits.mplot3d import Axes3D

        fig = plt.figure(figsize=(10, 8))
        ax = fig.add_subplot(111, projection='3d')

        for k, col in zip(unique_labels, colors):
            if k == -1:
                col = [0, 0, 0, 1]

            class_member_mask = (labels == k)

            ax.scatter(X_umap_3d[class_member_mask, 0], X_umap_3d[class_member_mask, 1], X_umap_3d[class_member_mask, 2],
                    c=[tuple(col)],
                    edgecolor='k',
                    s=50)

        ax.set_title('DBSCAN Clusters Visualized using UMAP in 3D')
        plt.savefig('DBSCAN_Clusters_Visualized_using_UMAP_3D.png')
        plt.show()

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
