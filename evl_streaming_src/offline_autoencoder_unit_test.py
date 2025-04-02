import pickle
import numpy as np
import tensorflow as tf
import pandas as pd
import matplotlib.pyplot as plt
import os
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from keras.losses import MeanSquaredError
from sklearn.metrics import precision_score, recall_score, f1_score

# Helper functions for anomaly detection
def map_bit_positions_to_chunks(anomaly_positions, chunk_size=256):
    chunk_indices = set()
    for pos in anomaly_positions:
        # Convert position to integer if it's a string
        if isinstance(pos, str):
            pos = int(pos)
        chunk_idx = int(pos) // chunk_size
        chunk_indices.add(chunk_idx)
    return chunk_indices

def compare_anomalies(filename, ground_truth_positions, detected_chunks, chunk_size=256):
    true_chunks = map_bit_positions_to_chunks(ground_truth_positions, chunk_size)
    max_chunk = max(max(true_chunks, default=0), max(detected_chunks, default=0)) + 1
    true_binary = [1 if i in true_chunks else 0 for i in range(max_chunk)]
    pred_binary = [1 if i in detected_chunks else 0 for i in range(max_chunk)]
    return list(true_chunks), detected_chunks, true_binary, pred_binary

def determine_optimal_threshold(reconstruction_errors):
    all_errors = [error for errors in reconstruction_errors.values() for error in errors]
    thresholds = {
        'percentile_95': np.percentile(all_errors, 95),
        'percentile_90': np.percentile(all_errors, 90),
        'percentile_80': np.percentile(all_errors, 80),
        'percentile_70': np.percentile(all_errors, 70),
        'percentile_60': np.percentile(all_errors, 60),
        'percentile_50': np.percentile(all_errors, 50),  # Try median as threshold
        'mean_2std': np.mean(all_errors) + 2 * np.std(all_errors),
        'mean_1std': np.mean(all_errors) + 1 * np.std(all_errors),
        'mean_0.5std': np.mean(all_errors) + 0.5 * np.std(all_errors)
    }
    
    for name, threshold in thresholds.items():
        anomalies_detected = sum(1 for e in all_errors if e > threshold)
        print(f"{name}: {threshold:.6f} - Anomalies detected: {anomalies_detected}")
    
    return thresholds['percentile_50']  

def visualize_errors(reconstruction_errors, threshold, output_file='error_distribution.png'):
    plt.figure(figsize=(10, 6))
    all_errors = [error for errors in reconstruction_errors.values() for error in errors]
    
    plt.hist(all_errors, bins=50, alpha=0.75)
    plt.axvline(threshold, color='r', linestyle='--', label=f'Threshold: {threshold:.6f}')
    plt.title('Distribution of Reconstruction Errors')
    plt.xlabel('Reconstruction Error')
    plt.ylabel('Frequency')
    plt.legend()
    
    stats_text = (
        f"Min: {min(all_errors):.6f}\n"
        f"Max: {max(all_errors):.6f}\n"
        f"Mean: {np.mean(all_errors):.6f}\n"
        f"Std: {np.std(all_errors):.6f}\n"
        f"Anomalies: {sum(1 for e in all_errors if e > threshold)}"
    )
    plt.annotate(stats_text, xy=(0.75, 0.75), xycoords='axes fraction', 
                    bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.8))
    
    # Save and show
    plt.tight_layout()
    plt.savefig(output_file)
    plt.show()

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
print("Test Data:")
print(test_data.head())

# Load anomalies data from pickle file
with open(anomalies_path, 'rb') as file:
    ground_truth_anomalies = pickle.load(file)

print('************ Anomalies Data ************')
print(ground_truth_anomalies.shape)
print("Ground Truth Anomalies:")
print(ground_truth_anomalies.head())

reconstruction_errors = {}
predictions = {}
runtimes = []

## comment of debugging 
# os.chdir(os.getcwd() + '/evl_streaming_src/demo')

os.chdir(os.getcwd() + '/demo')
print(os.getcwd())

print("************ Enter Mode ************")
mode = input("Enter mode ('unit_test' or nothing to run full test): ").strip().lower()

if mode == 'unit_test':
    print("Running in unit test mode with 100 items...")
    test_data = test_data[:100]
else:
    print("Running full simulation...")

max_bit_number = test_data['bit_number'].max()

def process_row(row):
    start_time = time.time()
    bit_number = float(row['bit_number']) / max_bit_number
    bit_number_scaled = float(row['bit_number_scaled'])
    
    X_row = np.array([bit_number_scaled], dtype=np.float32)
    X_row = np.expand_dims(X_row, axis=0)  

    prediction = model.predict(X_row, verbose=0)
    
    error = np.mean(np.square(X_row - prediction))
    end_time = time.time()
    
    return row['filename'], prediction.flatten(), error, end_time - start_time

with ThreadPoolExecutor() as executor:
    futures = {executor.submit(process_row, row): row for _, row in test_data.iterrows()}
    for future in as_completed(futures):
        filename, prediction, error, runtime = future.result()
        runtimes.append(runtime)

        if filename not in reconstruction_errors:
            reconstruction_errors[filename] = []
            predictions[filename] = []
        reconstruction_errors[filename].append(error)
        predictions[filename].append(prediction)

threshold = determine_optimal_threshold(reconstruction_errors)
print(f"Using threshold: {threshold}")

visualize_errors(reconstruction_errors, threshold)

all_true_binary = []
all_pred_binary = []
file_results = []

print("\nComparing with ground truth anomalies...")
for idx, (filename, row) in enumerate(ground_truth_anomalies.iterrows()):
    try:
        anomaly_positions = row['anomaly_positions']
        if isinstance(anomaly_positions, str):
            if anomaly_positions.startswith('['):
                anomaly_positions = json.loads(anomaly_positions)
            else:
                anomaly_positions = [int(pos) for pos in anomaly_positions.split(',')]
        
        file_errors = reconstruction_errors.get(filename, [])
        
        detected_chunks = [i for i, error in enumerate(file_errors) if error > threshold]
        
        true_chunks, detected_chunks, true_binary, pred_binary = compare_anomalies(
            filename, anomaly_positions, detected_chunks)
        
        all_true_binary.extend(true_binary)
        all_pred_binary.extend(pred_binary)
        
        file_results.append({
            'filename': row['filename'],
            'true_chunks': true_chunks,
            'detected_chunks': detected_chunks,
            'true_positives': sum(1 for i in range(len(true_binary)) if true_binary[i] == 1 and pred_binary[i] == 1),
            'false_positives': sum(1 for i in range(len(true_binary)) if true_binary[i] == 0 and pred_binary[i] == 1),
            'false_negatives': sum(1 for i in range(len(true_binary)) if true_binary[i] == 1 and pred_binary[i] == 0)
        })
        
        if idx < 5 or len(detected_chunks) > 0:
            print(f"\nFile: {row['filename'],}")
            print(f"  Ground truth chunks: {true_chunks}")
            print(f"  Detected chunks: {detected_chunks}")
            if file_errors:
                print(f"  Sample errors: {[round(e, 6) for e in file_errors[:5]]}...")
    
    except Exception as e:
        print(f"Error processing anomalies for {filename}: {e}")
        print(f"Raw value: {row['anomaly_positions']}")

if all_true_binary and all_pred_binary:
    precision = precision_score(all_true_binary, all_pred_binary, zero_division=0)
    recall = recall_score(all_true_binary, all_pred_binary, zero_division=0)
    f1 = f1_score(all_true_binary, all_pred_binary, zero_division=0)

    print("\nOverall Performance Metrics:")
    print(f"  Total ground truth anomalies: {sum(all_true_binary)}")
    print(f"  Total detected anomalies: {sum(all_pred_binary)}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall: {recall:.4f}")
    print(f"  F1 Score: {f1:.4f}")
else:
    print("\nNo valid binary classification arrays to calculate metrics.")

runtime_summary = {
    'min': np.min(runtimes),
    'max': np.max(runtimes),
    'median': np.median(runtimes),
    'mean': np.mean(runtimes),
    '25th_percentile': np.percentile(runtimes, 25),
    '75th_percentile': np.percentile(runtimes, 75),
    'standard_deviation': np.std(runtimes)
}

print("\nStatistical Summary of Runtimes:")
for key, value in runtime_summary.items():
    print(f"{key.capitalize()}: {value:.2f} seconds")

plt.figure(figsize=(10, 6))
plt.boxplot(runtimes, vert=False, patch_artist=True)
plt.title('Boxplot of Runtimes')
plt.xlabel('Runtime (seconds)')
plt.savefig('UA_JITC_runtime_boxplot.png')
plt.show()

results_df = pd.DataFrame(file_results)
print("\nResults summary by file:")
print(results_df.head())

if mode != 'unit_test':  
    results_df.to_csv('UA_JITC_anomaly_detection_results.csv', index=False)
    print("Results saved to UA_JITC_anomaly_detection_results.csv")