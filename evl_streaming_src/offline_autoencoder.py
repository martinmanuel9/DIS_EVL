import pickle
import numpy as np
import pandas as pd
import tensorflow as tf
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

# Process each file individually to calculate overall reconstruction errors
for filename, bit_number in test_dict.items():
    # Store the length of the current array
    lengths[filename] = len(bit_number)
    
    # Reshape the input to match the model's expected input shape
    X_row = np.array(bit_number).reshape(-1, 1)  # Adjust this reshape according to the model's input shape
    
    # Predict using the autoencoder model
    prediction = model.predict(X_row)
    
    # Calculate the reconstruction error (MSE for this sample)
    error = np.mean(np.square(X_row - prediction))
    reconstruction_errors[filename] = error

# Define the threshold based on the 95th percentile of overall reconstruction errors
threshold = np.percentile(list(reconstruction_errors.values()), 95)

# Now process each file again to determine which elements are anomalous
for filename, bit_number in test_dict.items():
    # Reshape the input to match the model's expected input shape
    X_row = np.array(bit_number).reshape(-1, 1)
    
    # Predict using the autoencoder model
    prediction = model.predict(X_row)
    
    # Calculate individual reconstruction errors for each element in the array
    errors = np.square(X_row - prediction).flatten()

    # Determine anomalies based on the threshold and identify which elements are anomalous
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

# Optionally, print all anomalies detected
print("Total Anomalies Detected:", len(anomalous_files))
print("Anomalous Files and Their Anomalous Elements:")
for filename, error in anomalous_files:
    print(f"File {filename}:")
    print(f" - Length of Array: {lengths[filename]}")
    print(f" - Reconstruction Error: {error}")
    print(f" - Anomalous Elements: {anomalous_elements_dict[filename]}")

# If you need to save the results to a file
import pandas as pd

results_df = pd.DataFrame({
    'filename': list(anomalies.keys()),
    'array_length': list(lengths.values()),
    'reconstruction_error': list(reconstruction_errors.values()),
    'is_anomaly': list(anomalies.values()),
    'anomalous_elements': [anomalous_elements_dict.get(f, []) for f in anomalies.keys()]
})

results_df.to_csv('anomaly_detection_results.csv', index=False)
