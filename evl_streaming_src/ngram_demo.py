#set drive a file manager
from collections import Counter
from nltk.util import ngrams
import pandas as pd
import numpy as np
import joblib
import os
import json
from tensorflow.keras.models import load_model

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', None)


model_file_path = os.getcwd() + '/evl_streaming_src/models/autoencoder_model.keras'
weights_file_path = os.getcwd() + '/evl_streaming_src/models/autoencoder.weights.h5'
scalar_file_path = os.getcwd() + '/evl_streaming_src/models/scaler.pkl'
# Load the architecture of the model
autoencoder = load_model(model_file_path)
# Load the weights from the separate file
autoencoder.load_weights(weights_file_path)
# Load the scaler from the file
scaler = joblib.load(scalar_file_path)

ngrams_file_path = os.getcwd() + '/evl_streaming_src/datasets/ngrams.csv'

ngram_df = pd.read_csv(ngrams_file_path)
ngram_df['ngram'] = ngram_df['ngram'].apply(lambda x: str(x).zfill(16))

ngram_df.head()

def get_ngrams(sequence, n):
    """
    Generate n-grams from a given sequence.
    """
    ngrams = [sequence[i:i+n] for i in range(len(sequence) - n + 1)]
    return ngrams

def process_json_file(file_path, n=16):
    """
    Read JSON file, extract data, generate n-grams and their frequencies.
    """
    with open(file_path, 'r') as file:
        data = json.load(file)
    ngrams = get_ngrams(data, n)
    ngram_freq = Counter(ngrams)
    return ngram_freq

def process_all_files_in_directory(directory, n, ngram_df):
    """
    Process all JSON files in a directory and its subdirectories.
    """
    expected_ngrams = ngram_df['ngram'].tolist()  # List of all expected n-grams
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                data_list = []
                file_path = os.path.join(root, file)
                ngram_freq = process_json_file(file_path, n)
                # Create a dictionary to store the data along with the file path
                data = {'file_path': file_path}
                data.update(ngram_freq)
                data_list.append(data)
                # Create a DataFrame from the list of dictionaries
                df = pd.DataFrame(data_list).fillna(0)
                # Ensure df has all expected columns
                df = df.set_index('file_path')
                df = df.reindex(columns=expected_ngrams, fill_value=0)
                df.reset_index(inplace=True)
                # Convert all columns except 'file_path' to numeric
                df[df.columns.difference(['file_path'])] = df[df.columns.difference(['file_path'])].apply(pd.to_numeric)
                # Sort the columns
                sorted_columns = sorted(df.drop(columns=['file_path']).columns, key=lambda x: int(x, 2))
                df = df[sorted_columns]
                # Transform the data
                test = scaler.transform(df)
                # Proceed with prediction
                reconstructions = autoencoder.predict(test, verbose=0)
                reconstruction_errors = np.mean(np.abs(test - reconstructions), axis=1)
                threshold = np.percentile(reconstruction_errors, 95)
                anomaly = threshold > 0.1
                if anomaly:
                    number = file_path.split('/')[-1]
                    print(f"Anomaly detected in file: {number}")
                    binary_string = read_binary_file_as_bits(file_path)
                    ngrams = create_16bit_ngrams(binary_string, ngram_df)


# Step 1: Write JSON data to a binary file
def write_json_to_binary_file(json_data, binary_file_name):
    with open(binary_file_name, 'wb') as bin_file:
        json_string = json.dumps(json_data)
        bin_file.write(json_string.encode('utf-8'))

# Step 2: Read binary file and convert to binary string
def read_binary_file_as_bits(binary_file_name):
    with open(binary_file_name, 'rb') as bin_file:
        binary_data = bin_file.read()
    # Convert binary data to a string of bits
    binary_string = ''.join(f'{byte:08b}' for byte in binary_data)
    return binary_string

# Step 3: Split binary string into 16-bit groups, shifting by 1 bit for each row
def create_dataframe_from_binary_string(binary_string):
    rows = []
    for i in range(len(binary_string) - 15):  # Ensure at least 16 bits are available
        row = list(binary_string[i:i+16])
        rows.append(row)

    # Create DataFrame, each row is a 16-bit group
    df = pd.DataFrame(rows, columns=[f'bit_{i}' for i in range(16)])
    return df

def create_16bit_ngrams(binary_str,df):
    ngrams = []
    # Move 1 bit at a time and extract 16-bit groups
    for i in range(len(binary_str) - 15):
        ngram = binary_str[i:i + 16]
        # ngrams.append(ngram)
        if ngram in df['ngram'].values:
            inx = df[df['ngram'] == ngram].index[0]
            status = df.loc[inx, 'type']
            if status == 'anomaly':
                print(f"{ngram} in position: {i+1} is {status}")
            # if df.loc[inx, 'ngram_type'] == 'anomaly_ngrams':
            #   print(f"status {inx+1}: {df.loc[inx, 'ngram_type']}")
            # else:
            #   print(f"status {inx+1}: {df.loc[inx, 'ngram_type']}")
    return ngrams



directory = os.getcwd() + '/data/JITC_Test/demo'
process_all_files_in_directory(directory, 16, ngram_df)