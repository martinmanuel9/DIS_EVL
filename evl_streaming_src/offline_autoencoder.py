import pickle
import numpy as np
import tensorflow as tf
import pandas as pd
import matplotlib.pyplot as plt
import os
import time  # To measure runtime
from keras.losses import MeanSquaredError

# Path to the autoencoder model
file_path = '/srv/docker/users/martinmlopez/DIS_EVL/evl_streaming_src/models/ae_offline_model_JITC.h5'

# Load the model with custom_objects for MeanSquaredError
model = tf.keras.models.load_model(file_path, custom_objects={'mse': MeanSquaredError()})

print(model.summary())

# Path to the test data
test_path = '/srv/docker/users/martinmlopez/DIS_EVL/evl_streaming_src/datasets/JITC_Test_Dataframe_offline.pkl'

# Load test data from pickle file
with open(test_path, 'rb') as file:
    test_data = pickle.load(file)

# Convert test data into a dictionary with filenames as keys
test_dict = {row['filename']: row['bit_number'] for _, row in pd.DataFrame(test_data).iterrows()}

# Initialize dictionaries to store results
reconstruction_errors = {}
anomalies = {}
anomalous_files = []
lengths = {}
anomalous_elements_dict = {}
runtimes = []  # To store runtimes for each sample

# Process each file individually to calculate overall reconstruction errors
for filename, bit_number in test_dict.items():
    # Store the length of the current array
    lengths[filename] = len(bit_number)
    
    # Reshape the input to match the model's expected input shape
    X_row = np.array(bit_number).reshape(-1, 1)
    
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
    reconstruction_errors[filename] = error

# Define the threshold based on the 95th percentile of overall reconstruction errors
threshold = np.percentile(list(reconstruction_errors.values()), 95)

# Now process each file again to determine which elements are anomalous
for filename, bit_number in test_dict.items():
    X_row = np.array(bit_number).reshape(-1, 1)
    prediction = model.predict(X_row)
    errors = np.square(X_row - prediction).flatten()
    anomalous_elements = np.where(errors > threshold)[0]

    if len(anomalous_elements) > 0:
        anomalies[filename] = True
        anomalous_files.append((filename, reconstruction_errors[filename]))
        anomalous_elements_dict[filename] = anomalous_elements.tolist()
    else:
        anomalies[filename] = False
        
    print(f"File {filename}:")
    print(f" - Length of Array: {lengths[filename]}")
    print(f" - Reconstruction Error: {reconstruction_errors[filename]}")
    print(f" - Anomaly Detected: {anomalies[filename]}")
    
    if anomalies[filename]:
        print(f" - Anomalous Elements: {anomalous_elements.tolist()}")

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
plt.savefig('JITC_runtime_boxplot.png')
plt.show()

# Optionally, print all anomalies detected
print("Total Anomalies Detected:", len(anomalous_files))
print("Anomalous Files and Their Anomalous Elements:")
for filename, error in anomalous_files:
    print(f"File {filename}:")
    print(f" - Length of Array: {lengths[filename]}")
    print(f" - Reconstruction Error: {error}")
    print(f" - Anomalous Elements: {anomalous_elements_dict[filename]}")

# Save the results to a file
results_df = pd.DataFrame({
    'filename': list(anomalies.keys()),
    'Num_of_8_Bytes_in_File': list(lengths.values()),
    'reconstruction_error': list(reconstruction_errors.values()),
    'is_anomaly': list(anomalies.values()),
    'Anomaly_Location': [anomalous_elements_dict.get(f, []) for f in anomalies.keys()],
    'runtime': runtimes
})

results_df.to_csv('JITC_anomaly_detection_results.csv', index=False)
