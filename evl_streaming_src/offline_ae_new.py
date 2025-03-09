import pickle
import numpy as np
import tensorflow as tf
import pandas as pd
import matplotlib.pyplot as plt
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from keras.losses import MeanSquaredError
from sklearn.metrics import precision_score, recall_score, f1_score

# Path to the autoencoder model
file_path = '/srv/docker/users/martinmlopez/DIS_EVL/evl_streaming_src/models/ae_offline_model_JITC.h5'

# Load the model with custom_objects for MeanSquaredError
model = tf.keras.models.load_model(file_path, custom_objects={'mse': MeanSquaredError()})

print(model.summary())

# Path to the test data
test_path = '/srv/docker/users/martinmlopez/DIS_EVL/evl_streaming_src/datasets/UA_JITC_test_Bits_Clustered_Dataframe.pkl'
anomalies_path = '/srv/docker/users/martinmlopez/DIS_EVL/evl_streaming_src/datasets/UA_JITC_anomalies.pkl'

# Load test data from pickle file
with open(test_path, 'rb') as file:
    test_data = pickle.load(file)

print('************ Test Data ************')
print(test_data.shape)

# Load anomalies data from pickle file
with open(anomalies_path, 'rb') as file:
    ground_truth_anomalies = pickle.load(file)

print('************ Anomalies Data ************')

# Initialize dictionaries to store results
reconstruction_errors = {}
predictions = {}
anomalous_elements_dict = {}
runtimes = []

# Define a scaling factor for normalization if needed
max_bit_number = test_data['bit_number'].max()

# Function to process a single row
def process_row(row):
    start_time = time.time()
    # Preprocess bit_number and bit_number_scaled
    bit_number = float(row['bit_number']) / max_bit_number
    bit_number_scaled = float(row['bit_number_scaled'])
    
    # Combine into an array
    X_row = np.array([bit_number, bit_number_scaled], dtype=np.float32)
    X_row = np.expand_dims(X_row, axis=0)  # Add batch dimension

    # Predict using the autoencoder model
    prediction = model.predict(X_row)
    
    # Calculate reconstruction error
    error = np.mean(np.square(X_row - prediction))
    end_time = time.time()
    
    return row['filename'], prediction.flatten(), error, end_time - start_time

# Parallel processing of rows
with ThreadPoolExecutor() as executor:
    futures = {executor.submit(process_row, row): row for _, row in test_data.iterrows()}
    for future in as_completed(futures):
        filename, prediction, error, runtime = future.result()
        runtimes.append(runtime)

        # Store results
        if filename not in reconstruction_errors:
            reconstruction_errors[filename] = []
            predictions[filename] = []
        reconstruction_errors[filename].append(error)
        predictions[filename].append(prediction)

# Define the threshold based on the 95th percentile of all reconstruction errors
threshold = np.percentile([error for errors in reconstruction_errors.values() for error in errors], 95)
print("Threshold:", threshold)

# Identify anomalies and group by filename
for filename, errors in reconstruction_errors.items():
    anomalies = [i for i, error in enumerate(errors) if error > threshold]
    anomalous_elements_dict[filename] = anomalies 

# Compare identified anomalies with ground truth
y_true = []
y_pred = []

def compare_anomalies(filename, ground_truth_positions, anomalous_dict):
    # Flatten the list of lists into a single list
    gt_positions = [int(pos) for sublist in ground_truth_positions for pos in sublist]
    id_positions = [int(pos) for pos in anomalous_dict.get(filename, [])]
    max_index = max(max(gt_positions, default=0), max(id_positions, default=0)) + 1
    true_binary = [1 if i in gt_positions else 0 for i in range(max_index)]
    pred_binary = [1 if i in id_positions else 0 for i in range(max_index)]
    return filename, gt_positions, id_positions, true_binary, pred_binary


with ThreadPoolExecutor() as executor:
    futures = []
    for filename, ground_truth_positions in ground_truth_anomalies.items():
        futures.append(
            executor.submit(compare_anomalies, filename, ground_truth_positions, anomalous_elements_dict)
        )

    for future in as_completed(futures):
        filename, gt_positions, id_positions, true_binary, pred_binary = future.result()
        y_true.extend(true_binary)
        y_pred.extend(pred_binary)
        print(f"Filename: {filename}")
        print(f"  Ground Truth Anomalies: {gt_positions}")
        print(f"  Identified Anomalies: {id_positions}")

# Calculate performance metrics
precision = precision_score(y_true, y_pred)
recall = recall_score(y_true, y_pred)
f1 = f1_score(y_true, y_pred)

print(f"\nPerformance Metrics:")
print(f"  Precision: {precision:.2f}")
print(f"  Recall: {recall:.2f}")
print(f"  F1 Score: {f1:.2f}")

# Generate runtime statistics
runtime_summary = {
    'min': np.min(runtimes),
    'max': np.max(runtimes),
    'median': np.median(runtimes),
    'mean': np.mean(runtimes),
    '25th_percentile': np.percentile(runtimes, 25),
    '75th_percentile': np.percentile(runtimes, 75),
    'standard_deviation': np.std(runtimes)
}

# Print the statistical summary
print("\nStatistical Summary of Runtimes:")
for key, value in runtime_summary.items():
    print(f"{key.capitalize()}: {value:.2f} seconds")

# Create a boxplot for runtimes
plt.figure(figsize=(10, 6))
plt.boxplot(runtimes, vert=False, patch_artist=True)
plt.title('Boxplot of Runtimes')
plt.xlabel('Runtime (seconds)')
plt.savefig('UA_JITC_runtime_boxplot.png')
plt.show()

results_df = pd.DataFrame({
    'filename': list(anomalies.keys()),
    # 'Num_of_8_Bytes_in_File': list(lengths.values()),
    'reconstruction_error': list(reconstruction_errors.values()),
    'is_anomaly': list(anomalies.values()),
    'Anomaly_Location': [anomalous_elements_dict.get(f, []) for f in anomalies.keys()],
    'runtime': runtimes
})

results_df.to_csv('UA_JITC_anomaly_detection_results.csv', index=False)