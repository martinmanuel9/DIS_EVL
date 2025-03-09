import pickle
import numpy as np
import tensorflow as tf
import pandas as pd
import matplotlib.pyplot as plt
import os
import time  # To measure runtime
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
print(test_data)
print(test_data.shape)
print(test_data.columns)

# Load anomalies data from pickle file
with open(anomalies_path, 'rb') as file:
    ground_truth_anomalies = pickle.load(file)

print('************ Anomalies Data ************')
print(ground_truth_anomalies)
print(ground_truth_anomalies.shape)
print(ground_truth_anomalies.columns)

# Initialize dictionaries to store results
reconstruction_errors = {}
anomalies = {}
anomalous_files = []
predictions = {}
anomalous_elements_dict = {}
runtimes = []  # To store runtimes for each sample

## Temp to test
## Select 100 items from test_dict
selected_test_data = test_data[:100]

# Define a scaling factor for normalization if needed
max_bit_number = test_data['bit_number'].max()
for _, row in test_data.iterrows():
    # Preprocess bit_number and bit_number_scaled
    bit_number = float(row['bit_number']) / max_bit_number
    bit_number_scaled = float(row['bit_number_scaled'])
    
    # Combine into an array
    X_row = np.array([bit_number, bit_number_scaled], dtype=np.float32)
    X_row = np.expand_dims(X_row, axis=0)  # Add batch dimension
    
    # Measure start time
    start_time = time.time()

    # Predict using the autoencoder model
    prediction = model.predict(X_row)
    
    # Measure end time and calculate runtime
    end_time = time.time()
    runtime = end_time - start_time
    runtimes.append(runtime)
    
    # Calculate the reconstruction error (MSE for this sample)
    error = np.mean(np.square(X_row - prediction))

    # Append results for this row's filename
    filename = row['filename']
    if filename not in reconstruction_errors:
        reconstruction_errors[filename] = []
        predictions[filename] = []
    reconstruction_errors[filename].append(error)
    predictions[filename].append(prediction.flatten())

# Define the threshold based on the 95th percentile of all reconstruction errors
threshold = np.percentile([error for errors in reconstruction_errors.values() for error in errors], 95)
print("Threshold:", threshold)

# Second pass: Identify anomalies and group by filename
for filename, errors in reconstruction_errors.items():
    predictions_per_file = predictions[filename]
    anomalies = []
    for i, error in enumerate(errors):
        if error > threshold:
            anomalies.append(i)  # Index of the 128-bit sequence marked as anomalous

    anomalous_elements_dict[filename] = anomalies

# Compare identified anomalies with ground truth
y_true = []
y_pred = []

for filename, ground_truth_positions in ground_truth_anomalies.items():
    # Get the identified anomalies for this file
    identified_positions = anomalous_elements_dict.get(filename, [])

    # Create binary arrays for comparison
    max_index = max(max(ground_truth_positions, default=0), max(identified_positions, default=0)) + 1
    true_binary = [1 if i in ground_truth_positions else 0 for i in range(max_index)]
    pred_binary = [1 if i in identified_positions else 0 for i in range(max_index)]

    # Append to overall lists
    y_true.extend(true_binary)
    y_pred.extend(pred_binary)

    print(f"Filename: {filename}")
    print(f"  Ground Truth Anomalies: {ground_truth_positions}")
    print(f"  Identified Anomalies: {identified_positions}")

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

#change to demo directory 
os.chdir('/srv/docker/users/martinmlopez/DIS_EVL/evl_streaming_src/demo')

# Print the statistical summary
print("\nStatistical Summary of Runtimes:")
for key, value in runtime_summary.items():
    print(f"{key.capitalize()}: {value:.2f} seconds")

# Create a boxplot (parata chart) for runtimes
plt.figure(figsize=(10, 6))
plt.boxplot(runtimes, vert=False, patch_artist=True)
plt.title('Boxplot of Runtimes for 500 Samples')
plt.xlabel('Runtime (seconds)')
plt.savefig('UA_JITC_runtime_boxplot.png')
plt.show()

# Optionally, print all anomalies detected
print("Total Anomalies Detected:", len(anomalous_files))
print("Anomalous Files and Their Anomalous Elements:")
for filename, error in anomalous_files:
    print(f"File {filename}:")
    # print(f" - Length of Array: {lengths[filename]}")
    print(f" - Reconstruction Error: {error}")
    print(f" - Anomalous Elements: {anomalous_elements_dict[filename]}")

# Save the results to a file
results_df = pd.DataFrame({
    'filename': list(anomalies.keys()),
    # 'Num_of_8_Bytes_in_File': list(lengths.values()),
    'reconstruction_error': list(reconstruction_errors.values()),
    'is_anomaly': list(anomalies.values()),
    'Anomaly_Location': [anomalous_elements_dict.get(f, []) for f in anomalies.keys()],
    'runtime': runtimes
})

results_df.to_csv('UA_JITC_anomaly_detection_results.csv', index=False)
