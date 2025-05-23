#!/usr/bin/env python

"""
Application:        JITC processing
File name:          jitc_datagen_upgrade.py
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
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import DBSCAN
import hdbscan
from sklearn.model_selection import train_test_split
import umap
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

class JITC_DATAOPS:
    def __init__(self, dataset, type):
        self.dataset = dataset
        self.type = type
        self.data = {}  # dict to store processed data
        self.dataframe = pd.DataFrame()
        self.clustered_dataframe = pd.DataFrame()

    def update_jsons(self, directory):
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                # Prepend and append {} to each file
                with open(os.path.join(directory, filename), 'r') as f:
                    data = f.read()
                    data = '{\"binary\":' + data + '}'
                with open(os.path.join(directory, filename), 'w') as f:
                    f.write(data)

    def change_directory(self):
        path = os.getcwd()
        print(f"Current working directory: {path}")
        changed_path = os.path.join(path, 'data', 'synthetic_jitc', f'{self.type}_dataset')
        os.chdir(changed_path)
        print(f"Changed working directory to: {os.getcwd()}")

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

                updated = False  # Flag to check if any conversion happened

                if 'binary' in data and isinstance(data['binary'], list):
                    bit_string = ''.join(str(bit) for bit in data['binary'])
                    data['binary'] = bit_string
                    updated = True
                else:
                    print(f"'binary' key missing or not a list in file {filename}")

                if 'anomaly_positions' in data and isinstance(data['anomaly_positions'], list):
                    bit_string = ''.join(str(bit) for bit in data['anomaly_positions'])
                    data['anomaly_positions'] = bit_string
                    updated = True
                else:
                    print(f"'anomaly_positions' key missing or not a list in file {filename}")

                if updated:
                    with open(output_path, 'w') as f_out:
                        json.dump(data, f_out)

    def process_directory(self, directory):
        with ThreadPoolExecutor() as executor:
            json_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith('.json')]
            executor.map(self.process_json_file, json_files)

    def process_json_file(self, json_file):
        try:
            with open(json_file, 'r') as f:
                json_data = json.load(f)  # Load JSON data

                # Process binary data for 128-bit sequences
                binary_sequence = json_data['binary']
                sequences = self.get_bit_sequences(binary_sequence)

                # Determine if there is an anomaly
                anomaly_positions = json_data.get('anomaly_positions', [])
                
                # build chunk labels 
                chunk_labels = []
                chunk_size = 256
                for i, seq in enumerate(sequences):
                    chunk_start = i * chunk_size
                    chunk_end   = chunk_start + chunk_size
                    # Check if ANY flipped bit is within [chunk_start, chunk_end)
                    has_anomaly = any(pos >= chunk_start and pos < chunk_end
                                    for pos in anomaly_positions)
                    chunk_labels.append(1 if has_anomaly else 0)

                # 4) Save to self.data
                self.data[os.path.basename(json_file)] = {
                    'binary': binary_sequence,
                    'sequences': sequences,
                    'anomaly_positions': anomaly_positions,
                    'chunk_labels': chunk_labels
                }
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error processing {json_file}: {e}")

    def get_bit_sequences(self, sequence):
        """
        Extract 128-bit sequences from a given binary sequence.
        """
        sequence_length = 256
        sequences = [sequence[i:i+sequence_length] for i in range(0, len(sequence) - sequence_length + 1, sequence_length)]
        return sequences

    def import_data(self):
        self.change_directory()
        self.process_directory(os.getcwd())
        records = []

        for filename, data in self.data.items():
            sequences = data['sequences']
            anomaly_positions = data['anomaly_positions']
            binary = data['binary']
            chunk_labels = data.get('chunk_labels', [])

            records.append({
                'filename': filename,
                'binary': binary,
                'sequences': sequences,
                'anomaly_positions': anomaly_positions,
                'chunk_labels': chunk_labels
            })

        # Create the DataFrame from the records
        self.dataframe = pd.DataFrame(records)

        # Create 'num_bytes' column
        self.dataframe['num_bytes'] = self.dataframe['binary'].apply(lambda x: len(x) // 8)

        # Ensure the 'dataframe' directory exists
        os.chdir('../')
        if not os.path.exists('dataframe'):
            os.mkdir('dataframe')
        os.chdir('dataframe')

    def dbscan_cluster(self, X_scaled_chunk, eps=0.1, min_samples=10):
        dbscan = DBSCAN(eps=eps, min_samples=min_samples)
        return dbscan.fit_predict(X_scaled_chunk)

    def develop_dataset(self):
        # Prepare lists to collect data from all files
        all_bit_numbers = []
        all_filenames = []
        all_sequences = []
        all_labels = [] # chunk labels
        type = self.type

        # Iterate over each row (file) in the DataFrame
        for index, row in self.dataframe.iterrows():
            sequences = row['sequences']
            filename = row['filename']
            chunk_lbl = row['chunk_labels']  # list of 0/1 for each chunk

            # Filter valid sequences
            valid_sequences = [seq for seq in sequences if isinstance(seq, str) and len(seq) == 256 and set(seq) <= {'0', '1'}]
            bit_numbers = [int(seq, 2) for seq in valid_sequences]
            
            # Make sure chunk_lbl has same length as valid_sequences
            # (skip any mismatch if needed)
            if len(chunk_lbl) != len(valid_sequences):
                print(f"Warning: mismatch in chunk count for {filename}")
                # handle mismatch as you see fit
                # e.g. chunk_lbl = chunk_lbl[:len(valid_sequences)]

            # 3) Append to “master” lists
            all_bit_numbers.extend(bit_numbers)
            all_filenames.extend([filename]*len(bit_numbers))
            all_sequences.extend(valid_sequences)
            all_labels.extend(chunk_lbl)  # push 0/1 labels

        # Create a single DataFrame with all bit numbers
        combined_df = pd.DataFrame({
            'filename': all_filenames,
            'bit_number': all_bit_numbers,
            'sequences': all_sequences,
            'chunk_label': all_labels
        })
        
        # Scale bit numbers
        scaler = MinMaxScaler()
        combined_df['bit_number_scaled'] = scaler.fit_transform(combined_df['bit_number'].values.reshape(-1, 1))

        # Apply DBSCAN clustering
        dbscan = hdbscan.HDBSCAN(min_cluster_size=5, metric='euclidean')
        combined_df['labels'] = dbscan.fit_predict(combined_df[['bit_number_scaled']])

        # Save this combined DataFrame for plotting
        self.clustered_dataframe = combined_df

        # Save the DataFrame to a pickle file
        os.chdir('../')
        if not os.path.exists('dataframe'):
            os.mkdir('dataframe')
        os.chdir('dataframe')
        self.clustered_dataframe.to_pickle(f'UA_JITC_{type}_Bits_Clustered_Dataframe.pkl')

        # Proceed to visualize the clustered data
        self.visualize_clusters()
        
        updated_dataframe = self.aggregate_to_dataframe()
        updated_dataframe.to_pickle(f'UA_JITC_{type}_Bits_Aggregated_Dataframe.pkl')
        
    def aggregate_to_dataframe(self):
        # aggregated_list = []
        # # Group by `filename` and collect arrays into a new DataFrame
        # for filename, group in self.clustered_dataframe.groupby('filename'):
        #     aggregated_list.append({
        #         'filename': filename,
        #         'bit_number': group['bit_number'].values,
        #         'bit_number_scaled': group['bit_number_scaled'].values,
        #         'labels': group['labels'].values
        #     })
        # # Convert to DataFrame
        # aggregated_df = pd.DataFrame(aggregated_list)
        # return aggregated_df
        
        # Aggregate data by `file_name` with arrays for each column
        aggregated_list = []
        for filename, group in self.clustered_dataframe.groupby('filename'):
            aggregated_list.append({
            'filename': filename,
            'bit_number': group['bit_number'].values,
            'bit_number_scaled': group['bit_number_scaled'].values,
            'labels': group['labels'].values,
            'sequences': group['sequences'].values
            })
        # Convert the aggregated list to a DataFrame
        aggregated_df = pd.DataFrame(aggregated_list)
        print(aggregated_df)
        # Explode each array column to align bit_number with labels and bit_number_scaled
        exploded_df = aggregated_df.explode(['bit_number', 'bit_number_scaled', 'sequences','labels'], ignore_index=True)
        return exploded_df

    def visualize_clusters(self):
        # Extract data for visualization
        bit_number_scaled = self.clustered_dataframe['bit_number_scaled'].values.reshape(-1, 1)
        labels = self.clustered_dataframe['labels'].values

        # Apply UMAP for dimensionality reduction to 2D
        reducer = umap.UMAP(n_components=2, random_state=42)
        X_umap = reducer.fit_transform(bit_number_scaled)

        # Plotting
        unique_labels = np.unique(labels)
        colors = [plt.cm.Spectral(each) for each in np.linspace(0, 1, len(unique_labels))]

        plt.figure(figsize=(12, 8))
        for k, col in zip(unique_labels, colors):
            if k == -1:
                col = [0, 0, 0, 1]  # Black for noise

            class_member_mask = (labels == k)
            plt.plot(X_umap[class_member_mask, 0], X_umap[class_member_mask, 1], 'o',
                    markerfacecolor=tuple(col),
                    markeredgecolor='k',
                    markersize=6,
                    label=f'Cluster {k}')

        plt.title('DBSCAN Clusters Across All Files')
        plt.xlabel('UMAP Component 1')
        plt.ylabel('UMAP Component 2')
        plt.legend()
        plt.savefig('DBSCAN_Clusters_All_Files_UMAP_2D.png')
        plt.show()


if __name__ == "__main__":
    dataOps = JITC_DATAOPS(dataset='UA_JITC', type='train')
    ## Run this function only once if needed
    # update_json_path = os.getcwd()
    # update_json_path = os.path.join(update_json_path, 'data', 'synthetic_jitc', 'train_dataset')
    # dataOps.update_jsons(update_json_path)
    ## Convert JSON bits to strings (run only if needed)
    # path = os.getcwd()
    # input_dir = os.path.join(path, 'data', 'synthetic_jitc', 'train_dataset')
    # output_dir = os.path.join(path, 'data', 'synthetic_jitc', 'train_dataset')
    # dataOps.convert_json_bits_to_string(input_dir=input_dir, output_dir=output_dir)
    ## Import data and develop dataset
    dataOps.import_data()
    dataOps.develop_dataset(