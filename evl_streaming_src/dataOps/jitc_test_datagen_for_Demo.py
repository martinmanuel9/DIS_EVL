import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import DBSCAN

class JITC_DATAGEN_TEST:
    def __init__(self):
        self.data = {}

    def change_directory(self):
        path = os.getcwd()
        # Debug mode: adjust path as necessary
        testPath = str(path) + '/data/JITC_Test/files/'
        os.chdir(testPath)

    def process_directory(self, directory):
        # Define the ranges of files you want to process
        ranges = [(1, 101), (2000, 2100), (3000, 3100), (4000, 4100), (5000, 5100)]
        selected_files = []

        for start, end in ranges:
            for i in range(start, end):
                filename = f"{i}.json"
                filepath = os.path.join(directory, filename)
                if os.path.exists(filepath):
                    selected_files.append(filepath)

        with ThreadPoolExecutor() as executor:
            executor.map(self.process_json_file, selected_files)

    def process_json_file(self, json_file):
        with open(json_file, 'r') as f:
            json_data = json.load(f)

            # Process binary data for 128-bit sequences
            binary_sequence = json_data['binary']
            sequences = self.get_bit_sequences(binary_sequence)

            # Store the binary data and 128-bit sequences
            self.data[os.path.basename(json_file)] = {
                'binary': binary_sequence,
                'sequences': sequences
            }

    def get_bit_sequences(self, sequence):
        sequence_length = 128
        sequences = [sequence[i:i + sequence_length] for i in range(0, len(sequence) - sequence_length + 1, sequence_length)]
        return sequences

    def minmax_scaler(self, array):
        scaler = MinMaxScaler()
        return scaler.fit_transform(array)

    def import_data(self):
        self.change_directory()
        self.process_directory(os.getcwd())

        bit_numbers_list = []
        bits_list = []
        filenames = []

        for filename, data in self.data.items():
            sequences = data['sequences']
            bit_numbers = []
            bits = []

            for sequence in sequences:
                if isinstance(sequence, str) and len(sequence) == 128 and set(sequence) <= {'0', '1'}:
                    number = int(sequence, 2)
                    bit_numbers.append(number)
                    bits.append(sequence)

            # Apply MinMaxScaler to the bit numbers for this file
            bit_numbers_array = np.array(bit_numbers).reshape(-1, 1)
            bit_numbers_scaled = self.minmax_scaler(bit_numbers_array).flatten()

            bit_numbers_list.append(bit_numbers_scaled)  # Store scaled bit numbers as array
            bits_list.append(np.array(bits))  # Store bits as an array of 128-bit strings
            filenames.append(filename)

        # Create the final DataFrame with the filenames, scaled bit numbers, and bits
        self.dataframe = pd.DataFrame({
            'filename': filenames,
            'bit_number': bit_numbers_list,
            'bits': bits_list
        })

        # Flatten the scaled bit numbers and apply DBSCAN clustering
        flattened_bit_numbers = np.concatenate(bit_numbers_list).reshape(-1, 1)
        n_jobs = os.cpu_count()
        chunk_size = len(flattened_bit_numbers) // n_jobs

        # Split the scaled data into chunks for parallel processing
        chunks = [flattened_bit_numbers[i:i + chunk_size] for i in range(0, len(flattened_bit_numbers), chunk_size)]

        labels = np.array([], dtype=int)

        with ThreadPoolExecutor(max_workers=n_jobs) as executor:
            futures = {executor.submit(self.dbscan_cluster, chunk): i for i, chunk in enumerate(chunks)}

            for future in as_completed(futures):
                chunk_labels = future.result()
                labels = np.concatenate((labels, chunk_labels))

        # Since labels are flattened, we need to split them back to match the original data structure
        split_labels = np.split(labels, np.cumsum([len(bits) for bits in bits_list])[:-1])
        self.dataframe['labels'] = split_labels

        if not os.path.exists('../dataframe'):
            os.mkdir('../dataframe')
        os.chdir('../dataframe/')
        self.dataframe.to_pickle('JITC_Test_Dataframe_offline' + '.pkl')

        os.chdir('../files')


    def dbscan_cluster(self, X):
        db = DBSCAN(eps=0.5, min_samples=5)
        return db.fit_predict(X)

# Create an instance of the class and call the import_data method to start the processing
jitc_test_data = JITC_DATAGEN_TEST()
jitc_test_data.import_data()
