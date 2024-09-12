import pickle
import os
import numpy as np
import tensorflow as tf
import pandas as pd
from keras.losses import MeanSquaredError

file_path = '/srv/docker/users/martinmlopez/DIS_EVL/evl_streaming_src/models/ae_offline_model_JITC.h5'

# Load the model with custom_objects
model = tf.keras.models.load_model(file_path, custom_objects={'mse': MeanSquaredError()})

print(model.summary())
test_path = '/srv/docker/users/martinmlopez/DIS_EVL/evl_streaming_src/datasets/JITC_Test_Number_Dataframe_Normalized.pkl'
# load test data 
with open(test_path, 'rb') as file:
    test_data = pickle.load(file)

df_bit_number = test_data['bit_number']
df_bit_number = pd.DataFrame(list(df_bit_number))
df_labels = test_data['labels']
df_labels = pd.DataFrame(list(df_labels))

# concat ngrams_freq and labels and keep column names and order of columns
test_dataset = pd.concat([df_bit_number, df_labels], axis=1)
# last column is label
test_dataset.columns = list(df_bit_number.columns) + ['label']
# reset index for dataframe normal dataset
test_dataset.reset_index(drop=True, inplace=True)

test = test_dataset

# train_data['label'] = 0  # assign label for cross validation function
X = test.drop(labels=['label'], axis=1).values
y = test['label']

# Predict with the model
predictions = model.predict(X)

# Calculate the reconstruction error (MSE for each sample)
reconstruction_errors = np.mean(np.square(X - predictions), axis=1)

# Define a threshold for anomaly detection (you may need to adjust this)
threshold = np.percentile(reconstruction_errors, 95)  # for example, 95th percentile

# Identify anomalies
anomalies = reconstruction_errors > threshold

print("Reconstruction Errors:", reconstruction_errors)
print("Anomalies Detected:", anomalies)
