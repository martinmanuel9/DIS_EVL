#!/usr/bin/env python

"""
Application:        JITC processing
File name:          jitc_datagen_bits.py
Author:             Martin Manuel Lopez
Creation:           09/09/2024

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
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import DBSCAN
from sklearn.model_selection import train_test_split
# !pip install umap-learn
import umap
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from collections import Counter

class JITC_DATAOPS:
    def __init__(self, dataset):
        self.dataset = dataset
        self.data = {}  # dict to store processed data
        self.dataframe = pd.DataFrame()
        

    def update_jsons(self, directory):
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                # Prepend and append {} to each file
                with open(directory + '/' + filename, 'r') as f:
                    data = f.read()
                    data = '{\"binary\":' + data + '}'
                with open(directory + '/' + filename, 'w') as f:
                    f.write(data)

    def change_directory(self):
        path = os.getcwd()
        print(path)
        changed_path = path + '/data/synthetic_jitc/test_dataset'
        os.chdir(changed_path)
        print(os.getcwd())
        

    def convert_json_bits_to_string(self, input_dir, output_dir):
        """
        Converts JSON files containing 'binary' as a list of bits to 'binary' as a string of bits.

        Args:
            input_dir (str): Directory containing the original JSON files.
            output_dir (str): Directory to save the updated JSON files.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for filename in os.listdir(input_dir):
            if filename.endswith(".json"):
                input_path = os.path.join(input_dir, filename)
                output_path = os.path.join(output_dir, filename)

                with open(input_path, 'r') as f_in:
                    data = json.load(f_in)

                if 'binary' in data and isinstance(data['binary'], list):
                    # Convert list of bits to string
                    bit_string = ''.join(str(bit) for bit in data['binary'])
                    data['binary'] = bit_string

                    with open(output_path, 'w') as f_out:
                        json.dump(data, f_out)
                else:
                    # Handle cases where 'binary' is not a list (optional)
                    print(f"'binary' key missing or not a list in file {filename}")


    def process_directory(self, directory):
        with ThreadPoolExecutor() as executor:
            json_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith('.json')]
            executor.map(self.process_json_file, json_files)

    def process_json_file(self, json_file):
        try:
            with open(json_file, 'r') as f:
                json_data = json.load(f)  # Load JSON data

                # Process binary data for 32-bit sequences
                binary_sequence = json_data['binary']
                sequences = self.get_bit_sequences(binary_sequence)

                # Identify repeating N-bit sequences
                sequences = self.get_repeating_sequences(sequences)

                # Store the binary data and repeating 32-bit sequences
                self.data[os.path.basename(json_file)] = {
                    'binary': binary_sequence,
                    'sequences': sequences
                }
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error processing {json_file}: {e}")

    def get_bit_sequences(self, sequence):
        """
        Extract 32-bit sequences (4 bytes) from a given binary sequence.
        """
        sequence_length = 128
        sequences = [sequence[i:i+sequence_length] for i in range(0, len(sequence) - sequence_length + 1, sequence_length)]
        return sequences

    def get_repeating_sequences(self, sequences):
        """
        Identify and return repeating 32-bit sequences.
        """
        sequence_counts = Counter(sequences)
        repeating_sequences = {seq: count for seq, count in sequence_counts.items() if count > 1}
        return repeating_sequences

    def import_data(self):
        self.change_directory()
        self.process_directory(os.getcwd())
        records = []

        for filename, data in self.data.items():
            sequences = data['sequences']
            bit_numbers = []
            bits = []

            for sequence in sequences:
                if isinstance(sequence, str) and len(sequence) == 128 and set(sequence) <= {'0', '1'}:
                    number = int(sequence, 2)
                    bit_numbers.append(number)
                    bits.append(sequence)

            records.append({
                'filename': filename,
                'bit_numbers': np.array(bit_numbers),
                'bits': np.array(bits),
                'sequences': sequences
            })

        # Create the DataFrame from the records
        self.dataframe = pd.DataFrame(records)

    def minmax_scaler(self, array_of_bits):
            scaler = MinMaxScaler()
            return scaler.fit_transform(array_of_bits)

    def dbscan_cluster(self, X_scaled_chunk, eps=0.1, min_samples=10):
        dbscan = DBSCAN(eps=eps, min_samples=min_samples)
        return dbscan.fit_predict(X_scaled_chunk)

    def develop_dataset(self):
        # Prepare data for clustering
        df_repeating_sequences = self.dataframe['sequences'].apply(lambda x: list(x.keys()))
        df_repeating_sequences = pd.DataFrame(list(df_repeating_sequences))

        # fill NaN values with 0
        df_repeating_sequences = df_repeating_sequences.fillna(0)
        filtered_df = df_repeating_sequences.loc[:, (df_repeating_sequences != 0).any(axis=0)]

        array_converted_bits = []

        # Iterate over each element in the DataFrame
        for column in filtered_df.columns:
            for item in filtered_df[column]:
                # Check if the item is a 32-bit string and contains only '0' and '1'
                if isinstance(item, str) and len(item) == 128 and set(item) <= {'0', '1'}:
                    # Convert the 32-bit string to a number
                    number = int(item, 2)
                    array_converted_bits.append(number)
        
        array_of_bits = []
        # Iterate over each element in the DataFrame
        for column in filtered_df.columns:
            for item in filtered_df[column]:
                # Check if the item is a 32-bit string and contains only '0' and '1'
                if isinstance(item, str) and len(item) == 128 and set(item) <= {'0', '1'}:
                    # Convert the 32-bit string to a number
                    array_of_bits.append(item)

        array_of_bits = np.array(array_of_bits)
        array_of_bits = array_of_bits.reshape(-1, 1)
        
        array_converted_bits = np.array(array_converted_bits)
        array_converted_bits = array_converted_bits.reshape(-1, 1)

        # Step 1: Standardize the data using MinMaxScaler (single-threaded)
        X_scaled = self.minmax_scaler(array_converted_bits)

        # Step 2: Run DBSCAN using ThreadPoolExecutor
        n_jobs = os.cpu_count()  # Get the number of available cores
        chunk_size = len(X_scaled) // n_jobs

        # Split the scaled data into chunks for parallel processing
        chunks = [X_scaled[i:i + chunk_size] for i in range(0, len(X_scaled), chunk_size)]

        labels = np.array([], dtype=int)

        with ThreadPoolExecutor(max_workers=n_jobs) as executor:
            futures = {executor.submit(self.dbscan_cluster, chunk): i for i, chunk in enumerate(chunks)}
            
            for future in as_completed(futures):
                chunk_labels = future.result()
                labels = np.concatenate((labels, chunk_labels))

        # Add labels to the dataframe
        # concatenate the X_scaled with the labels
        X_scaled_DF = pd.DataFrame(X_scaled, columns=['bit_number'])
        labels_DF = pd.DataFrame(labels, columns=['labels'])
        bits_DF = pd.DataFrame(array_of_bits, columns=['bits'])
        
        if isinstance(X_scaled, pd.Series):
            X_scaled_DF = X_scaled.to_frame(name='bit_number')
        if isinstance(array_of_bits, pd.Series):
            bits_DF = array_of_bits.to_frame(name='bits')
        if isinstance(labels, pd.Series):
            labels_DF = labels.to_frame(name='labels')
        
        self.dataframe = pd.concat([X_scaled_DF, bits_DF, labels_DF ], axis=1)
        
        bitnum_DF = self.dataframe[['bit_number', 'labels']]
        bits_DF = self.dataframe[['bits', 'labels']]
        
        # normalize dat
        scaler = MinMaxScaler()
        df_number_normalized = pd.DataFrame(scaler.fit_transform(bitnum_DF[['bit_number']]), columns=['bit_number'])
        # add the labels back
        df_number_normalized['labels'] = bitnum_DF['labels']

        # Save the dataframe
        if not os.path.exists('dataframe'):
            os.mkdir('dataframe')
        os.chdir('dataframe')
        self.dataframe.to_pickle('UA_JITC_Test_Bits_Dataframe.pkl')
        df_number_normalized.to_pickle('UA_JITC_Test_Number_Dataframe_Normalized.pkl')
        bits_DF.to_pickle('UA_JITC_Test_Bits_Dataframe_Normalized_Bits.pkl')
        os.chdir('../')

        # Step 3: Count unique labels (excluding noise)
        unique_labels = np.unique(labels)
        n_clusters = len(unique_labels)
        self.unique_labels = unique_labels
        self.nKlusters = n_clusters
        print(f"Number of clusters: {self.nKlusters}")
        print(f"Labels: {self.unique_labels}")

        # Apply UMAP to reduce to 2D
        reducer = umap.UMAP(n_components=2, random_state=42)
        X_umap = reducer.fit_transform(X_scaled_DF.values)

        # Plotting the UMAP results
        plt.figure(figsize=(8, 6))
        colors = [plt.cm.Spectral(each) for each in np.linspace(0, 1, len(unique_labels))]

        for k, col in zip(unique_labels, colors):
            if k == -1:
                col = [0, 0, 0, 1]  # Black used for noise (points labeled as -1)

            class_member_mask = (labels == k)
            plt.plot(X_umap[class_member_mask.flatten(), 0], X_umap[class_member_mask.flatten(), 1], 'o',
                    markerfacecolor=tuple(col),
                    markeredgecolor='k',
                    markersize=6)

        plt.title('DBSCAN 128-bit Sequence Clusters Visualized using UMAP')
        plt.xlabel('UMAP Component 1')
        plt.ylabel('UMAP Component 2')
        plt.savefig('DBSCAN_128bit_Clusters_Visualized_using_UMAP_2D.png')
        plt.show()

        # Apply UMAP to reduce to 3D
        reducer = umap.UMAP(n_components=3, random_state=42)
        X_umap_3d = reducer.fit_transform(X_scaled_DF.values)

        # 3D Plotting
        fig = plt.figure(figsize=(10, 8))
        ax = fig.add_subplot(111, projection='3d')

        for k, col in zip(unique_labels, colors):
            if k == -1:
                col = [0, 0, 0, 1]

            class_member_mask = (labels == k)

            ax.scatter(X_umap_3d[class_member_mask.flatten(), 0], X_umap_3d[class_member_mask.flatten(), 1], X_umap_3d[class_member_mask.flatten(), 2],
                        c=[tuple(col)],
                        edgecolor='k',
                        s=50)

        ax.set_title('DBSCAN 128-bit Sequence Clusters Visualized using UMAP in 3D')
        plt.savefig('DBSCAN_128bit_Clusters_Visualized_using_UMAP_3D.png')
        plt.show()

if __name__ == "__main__":
    dataOps = JITC_DATAOPS(dataset='UA_JITC')
    dataOps.import_data()
    #### need to run this function only once ####
    # dataOps.update_jsons(os.getcwd()) 
    #### need to run this function only once ####
    # path = os.getcwd()
    # input_dir = path + '/data/synthetic_jitc/train_dataset'
    # output_dir = path + '/data/synthetic_jitc/train_dataset'
    # dataOps.convert_json_bits_to_string(input_dir=input_dir, output_dir=output_dir)
    dataOps.develop_dataset()
