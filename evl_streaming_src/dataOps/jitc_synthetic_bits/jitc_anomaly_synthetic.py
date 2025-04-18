import os
import json
import random
import pandas as pd
import pickle

def inject_anomalies_to_test_dataset(train_dir, test_dir, num_files, min_anomalies=1, max_anomalies=5):
    """
    Reads JSON files from the train_dataset, injects anomalies, and saves them into the test_dataset.

    Args:
        train_dir (str): Directory containing the original JSON files (train_dataset).
        test_dir (str): Directory to save the modified JSON files with anomalies (test_dataset).
        num_files (int): Number of files to process.
        min_anomalies (int): Minimum number of anomalies to inject per file.
        max_anomalies (int): Maximum number of anomalies to inject per file.
    """
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    for i in range(1, num_files + 1):
        filename = f"file_{i}.json"
        input_path = os.path.join(train_dir, filename)
        output_path = os.path.join(test_dir, filename)

        if not os.path.exists(input_path):
            print(f"Skipping file {filename}: File does not exist in train_dataset")
            continue

        with open(input_path, 'r') as f_in:
            data = json.load(f_in)
            binary_str = data.get('binary', '')
            bit_length = len(binary_str)

            if bit_length == 0:
                print(f"Skipping file {filename}: Binary string is empty")
                continue

            # Determine number of anomalies to inject
            num_anomalies = random.randint(min_anomalies, max_anomalies)
            num_anomalies = min(num_anomalies, bit_length)  # Ensure we don't select more anomalies than bits

            # Randomly select positions to flip
            anomaly_positions = random.sample(range(bit_length), num_anomalies)

            # Convert string to list for mutation
            binary_list = list(binary_str)

            # Inject anomalies by flipping bits
            for pos in anomaly_positions:
                original_bit = binary_list[pos]
                flipped_bit = '0' if original_bit == '1' else '1'
                binary_list[pos] = flipped_bit

            # Convert list back to string
            modified_binary_str = ''.join(binary_list)

            # Save modified data with anomalies
            modified_data = {
                'binary': modified_binary_str,
                'anomaly_positions': anomaly_positions
            }

            with open(output_path, 'w') as f_out:
                json.dump(modified_data, f_out)

    print(f"Anomalies injected into {num_files} files and saved to {test_dir}.")
    
def extract_anomalies(anomalies_dir, output_pkl_file, num_files):
    """
    Extracts anomaly positions arrays from JSON files and saves to a DataFrame and .pkl file.

    Args:
        anomalies_dir (str): Directory containing the JSON files with anomalies.
        output_pkl_file (str): Path to the output .pkl file.
        num_files (int): Number of files to process.

    Returns:
        pandas.DataFrame: DataFrame containing the anomalies information.
    """
    anomalies_data = []

    for i in range(1, num_files + 1):
        filename = f"file_{i}.json"
        file_path = os.path.join(anomalies_dir, filename)

        if not os.path.exists(file_path):
            print(f"Skipping file {filename}: File does not exist in anomalies directory")
            continue

        with open(file_path, 'r') as f:
            data = json.load(f)
            anomaly_positions = data.get('anomaly_positions', [])

            # Add a record per file, with anomaly_positions as an array
            anomalies_data.append({
                'filename': filename,
                'anomaly_positions': anomaly_positions
            })

    # Create a DataFrame from the list of dictionaries
    anomalies_df = pd.DataFrame(anomalies_data)
    os.chdir(os.getcwd() + '/evl_streaming_src/datasets')
    # Save the DataFrame to a .pkl file
    anomalies_df.to_pickle(output_pkl_file)

    print(f"Anomalies data extracted and saved to {output_pkl_file}.")

    return anomalies_df

# Usage
if __name__ == "__main__":
    path = os.getcwd()
    train_dataset_dir = path + '/data/synthetic_jitc/train_dataset'  # Directory with original JSON files (normal data)
    test_dataset_dir = path + '/data/synthetic_jitc/test_dataset'    # Directory to save JSON files with anomalies
    num_files = 5000                     # Number of files to process
    min_anomalies = 10                    # Minimum anomalies per file
    max_anomalies = 30                    # Maximum anomalies per file
    output_pkl_file = "UA_JITC_anomalies.pkl"

    inject_anomalies_to_test_dataset(
        train_dir=train_dataset_dir,
        test_dir=test_dataset_dir,
        num_files=num_files,
        min_anomalies=min_anomalies,
        max_anomalies=max_anomalies
    )
    anomalies_df = extract_anomalies(anomalies_dir=test_dataset_dir, output_pkl_file=output_pkl_file, num_files=num_files)
