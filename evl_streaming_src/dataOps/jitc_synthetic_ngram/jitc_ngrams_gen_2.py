from collections import Counter
from nltk.util import ngrams
import pandas as pd
import numpy as np
import os
import json
import pandas as pd
import numpy as np

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
            status = df.loc[inx, 'ngram_type']
            if status == 'anomaly_ngrams':
                print(f"{ngram} in position: {i+1} is {status}")
            # if df.loc[inx, 'ngram_type'] == 'anomaly_ngrams':
            #   print(f"status {inx+1}: {df.loc[inx, 'ngram_type']}")
            # else:
            #   print(f"status {inx+1}: {df.loc[inx, 'ngram_type']}")
    return ngrams


pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', None)

path = os.getcwd() + '/data/JITC_Data/unique_ngrams.csv'
df = pd.read_csv(path)

labels = df['ngram_type']

df['ngram'] = df['ngram'].apply(lambda x: str(x).zfill(16))

df['ngram_type'].unique()

df.head(3)

# Specify the directory containing the JSON files
directory = os.getcwd() + '/data/JITC_Test/demo/1.json'
# Step 2: Read the binary file as bits
binary_string = read_binary_file_as_bits(directory)

df.loc[2, 'ngram_type']
ngrams = create_16bit_ngrams(binary_string, df)

ngrams