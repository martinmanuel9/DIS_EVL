#!/usr/bin/env python

"""
Application:        Offline Autoencoder Unit Test
File name:          offline_autoencoder_unit_test.py
Author:             Martin Manuel Lopez
Creation:           April 10, 2025

The University of Arizona
Department of Electrical and Computer Engineering
College of Engineering
"""

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
from classifier_performance import PerformanceMetrics

def parse_anomaly_positions(anomaly_positions):
    if anomaly_positions is None:
        return []
        
    # Already a list
    if isinstance(anomaly_positions, list):
        return [int(pos) for pos in anomaly_positions]
    
    # String representation of a JSON array
    if isinstance(anomaly_positions, str):
        if anomaly_positions.startswith('['):
            try:
                return [int(pos) for pos in json.loads(anomaly_positions)]
            except json.JSONDecodeError:
                print(f"Error parsing JSON array: {anomaly_positions}")
                return []
        elif ',' in anomaly_positions:
            # Comma-separated string
            return [int(pos) for pos in anomaly_positions.split(',')]
        else:
            # String of concatenated digits (unlikely but possible)
            try:
                # Try to interpret as a list of positions
                return [int(anomaly_positions)]
            except ValueError:
                print(f"Could not parse anomaly positions: {anomaly_positions}")
                return []
    
    print(f"Unknown format for anomaly positions: {type(anomaly_positions)}")
    return []

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
    if not isinstance(ground_truth_positions, list):
        ground_truth_positions = parse_anomaly_positions(ground_truth_positions)
    
    # Convert individual bit positions to chunk indices
    true_chunks = map_bit_positions_to_chunks(ground_truth_positions, chunk_size)
    
    # Prepare binary arrays for metrics calculation
    max_chunk = max(max(true_chunks, default=0), max(detected_chunks, default=0)) + 1
    
    true_binary = [1 if i in true_chunks else 0 for i in range(max_chunk)]
    pred_binary = [1 if i in detected_chunks else 0 for i in range(max_chunk)]
    
    return list(true_chunks), detected_chunks, true_binary, pred_binary

def calculate_metrics(true_binary, pred_binary, dataset_name="JITC", method_name="Autoencoder"):
    """
    Calculate performance metrics using the PerformanceMetrics class
    """
    true_binary = np.array(true_binary)
    pred_binary = np.array(pred_binary)
    
    start_time = time.time()
    # Perform your model evaluation here
    end_time = time.time()
    
    perf = PerformanceMetrics(
        tstart=start_time,
        tend=end_time,
        timestep=0, 
        preds=pred_binary,
        test=true_binary,
        dataset=dataset_name,
        method=method_name,
        classifier="tcis_autoencoder"
    )
    
    metrics = perf.findClassifierMetrics(pred_binary, true_binary)
    
    return metrics

def get_thresholds(reconstruction_errors):
    all_errors = [error for errors in reconstruction_errors.values() for error in errors]
    
    percentiles = [99, 95, 90, 85, 80, 75, 70, 65, 60, 55, 50]
    
    thresholds = {
        f'percentile_{p}': np.percentile(all_errors, p) for p in percentiles
    }
    
    thresholds.update({
        'mean_3std': np.mean(all_errors) + 3 * np.std(all_errors),
        'mean_2std': np.mean(all_errors) + 2 * np.std(all_errors),
        'mean_1std': np.mean(all_errors) + 1 * np.std(all_errors),
        'mean_0.5std': np.mean(all_errors) + 0.5 * np.std(all_errors),
        'mean': np.mean(all_errors)
    })
    
    for name, threshold in thresholds.items():
        anomalies_detected = sum(1 for e in all_errors if e > threshold)
        print(f"{name}: {threshold:.6f} - Anomalies detected: {anomalies_detected}/{len(all_errors)} ({anomalies_detected/len(all_errors)*100:.2f}%)")
    
    return thresholds

def visualize_errors(reconstruction_errors, thresholds, output_dir='plots'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    all_errors = [error for errors in reconstruction_errors.values() for error in errors]
    
    plt.figure(figsize=(12, 8))
    n, bins, patches = plt.hist(all_errors, bins=50, alpha=0.75, color='skyblue')
    
    colors = plt.cm.tab10(np.linspace(0, 1, len(thresholds)))
    
    for i, (name, threshold) in enumerate(thresholds.items()):
        plt.axvline(threshold, color=colors[i], linestyle='--', 
                    label=f'{name}: {threshold:.6f}')
    
    plt.title('Distribution of Reconstruction Errors with Multiple Thresholds')
    plt.xlabel('Reconstruction Error')
    plt.ylabel('Frequency')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    
    stats_text = (
        f"Min: {min(all_errors):.6f}\n"
        f"Max: {max(all_errors):.6f}\n"
        f"Mean: {np.mean(all_errors):.6f}\n"
        f"Std: {np.std(all_errors):.6f}\n"
    )
    plt.annotate(stats_text, xy=(0.75, 0.9), xycoords='axes fraction', 
                bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.8))
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'error_distribution_all_thresholds.png'))
    plt.close()
    
    for name, threshold in thresholds.items():
        plt.figure(figsize=(10, 6))
        plt.hist(all_errors, bins=50, alpha=0.75, color='skyblue')
        plt.axvline(threshold, color='r', linestyle='--', 
                    label=f'Threshold: {threshold:.6f}')
        
        anomalies_detected = sum(1 for e in all_errors if e > threshold)
        plt.title(f'Threshold: {name} ({anomalies_detected} anomalies detected)')
        plt.xlabel('Reconstruction Error')
        plt.ylabel('Frequency')
        plt.legend()
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'error_distribution_{name}.png'))
        plt.close()

def evaluate_with_threshold(threshold, reconstruction_errors, ground_truth_anomalies, threshold_name="custom"):
    all_true_binary = []
    all_pred_binary = []
    file_results = []
    
    print(f"\nEvaluating with threshold: {threshold_name} ({threshold:.6f})")
    
    for filename, row in ground_truth_anomalies.iterrows():
        if filename not in reconstruction_errors:
            continue
            
        try:
            # Parse anomaly positions
            anomaly_positions = parse_anomaly_positions(row['anomaly_positions'])
            file_errors = reconstruction_errors.get(filename, [])
            detected_chunks = [i for i, error in enumerate(file_errors) if error > threshold]
            
            true_chunks, detected_chunks, true_binary, pred_binary = compare_anomalies(
                filename, anomaly_positions, detected_chunks)
            
            tp = sum(1 for i in range(len(true_binary)) if true_binary[i] == 1 and pred_binary[i] == 1)
            fp = sum(1 for i in range(len(true_binary)) if true_binary[i] == 0 and pred_binary[i] == 1)
            fn = sum(1 for i in range(len(true_binary)) if true_binary[i] == 1 and pred_binary[i] == 0)
            
            all_true_binary.extend(true_binary)
            all_pred_binary.extend(pred_binary)
            
            file_results.append({
                'filename': filename,
                'threshold_name': threshold_name,
                'threshold_value': threshold,
                'num_true_chunks': len(true_chunks),
                'num_detected_chunks': len(detected_chunks),
                'true_positives': tp,
                'false_positives': fp,
                'false_negatives': fn
            })
        
        except Exception as e:
            print(f"Error processing anomalies for {filename}: {e}")
    
    metrics = {
        'threshold_name': threshold_name,
        'threshold_value': threshold,
        'total_files': len(file_results),
        'total_true_chunks': sum(row['num_true_chunks'] for row in file_results),
        'total_detected_chunks': sum(row['num_detected_chunks'] for row in file_results),
        'total_true_positives': sum(row['true_positives'] for row in file_results),
        'total_false_positives': sum(row['false_positives'] for row in file_results),
        'total_false_negatives': sum(row['false_negatives'] for row in file_results)
    }
    
    if all_true_binary and all_pred_binary:
        metrics['precision'] = precision_score(all_true_binary, all_pred_binary, zero_division=0)
        metrics['recall'] = recall_score(all_true_binary, all_pred_binary, zero_division=0)
        metrics['f1_score'] = f1_score(all_true_binary, all_pred_binary, zero_division=0)
        
        additional_metrics = calculate_metrics(
            all_true_binary, 
            all_pred_binary,
            dataset_name="JITC",
            method_name=f"Autoencoder-{threshold_name}"
        )
        
        metrics['classifier_error'] = additional_metrics['Classifier_Error']
        metrics['classifier_accuracy'] = additional_metrics['Classifier_Accuracy']
        metrics['matthews_corrcoef'] = additional_metrics['Matthews_CorrCoef']
        
        if additional_metrics['ROC_AUC_Score'] != 'Only one class found':
            metrics['roc_auc_score'] = additional_metrics['ROC_AUC_Score']
        
        print(f"  Files analyzed: {metrics['total_files']}")
        print(f"  Total ground truth chunks: {metrics['total_true_chunks']}")
        print(f"  Total detected chunks: {metrics['total_detected_chunks']}")
        print(f"  Precision: {metrics['precision']:.4f}")
        print(f"  Recall: {metrics['recall']:.4f}")
        print(f"  F1 Score: {metrics['f1_score']:.4f}")
        print(f"  Matthews Correlation Coefficient: {metrics['matthews_corrcoef']:.4f}")
        print(f"  Classifier Accuracy: {metrics['classifier_accuracy']:.4f}")
    else:
        metrics['precision'] = 0
        metrics['recall'] = 0
        metrics['f1_score'] = 0
        metrics['matthews_corrcoef'] = 0
        metrics['classifier_accuracy'] = 0
        print("  No valid binary classification arrays to calculate metrics.")
    
    return {
        'metrics': metrics,
        'file_results': file_results
    }

def run_batch_evaluation(reconstruction_errors, ground_truth_anomalies, output_dir='results'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    thresholds = get_thresholds(reconstruction_errors)
    
    visualize_errors(reconstruction_errors, thresholds, os.path.join(output_dir, 'plots'))
    
    all_metrics = []
    all_file_results = []
    
    for threshold_name, threshold_value in thresholds.items():
        results = evaluate_with_threshold(
            threshold_value, 
            reconstruction_errors, 
            ground_truth_anomalies,
            threshold_name
        )
        
        all_metrics.append(results['metrics'])
        all_file_results.extend(results['file_results'])
        
        pd.DataFrame(results['file_results']).to_csv(
            os.path.join(output_dir, f'file_results_{threshold_name}.csv'),
            index=False
        )
    
    pd.DataFrame(all_metrics).to_csv(
        os.path.join(output_dir, 'threshold_metrics_summary.csv'),
        index=False
    )
    
    pd.DataFrame(all_file_results).to_csv(
        os.path.join(output_dir, 'all_file_results.csv'),
        index=False
    )
    
    plot_metrics_comparison(all_metrics, os.path.join(output_dir, 'plots'))
    
    return all_metrics

def plot_metrics_comparison(metrics_list, output_dir='plots'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    metrics_df = pd.DataFrame(metrics_list)
    
    metrics_df = metrics_df.sort_values('threshold_value', ascending=False)
    
    plt.figure(figsize=(12, 8))
    plt.plot(metrics_df['threshold_name'], metrics_df['precision'], 'o-', label='Precision')
    plt.plot(metrics_df['threshold_name'], metrics_df['recall'], 'o-', label='Recall')
    plt.plot(metrics_df['threshold_name'], metrics_df['f1_score'], 'o-', label='F1 Score')
    
    plt.title('Performance Metrics for Different Thresholds')
    plt.xlabel('Threshold')
    plt.ylabel('Score')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'threshold_comparison_metrics.png'))
    plt.close()
    
    plt.figure(figsize=(12, 8))
    plt.plot(metrics_df['threshold_name'], metrics_df['classifier_accuracy'], 'o-', label='Accuracy')
    plt.plot(metrics_df['threshold_name'], metrics_df['matthews_corrcoef'], 'o-', label='Matthews Correlation')
    
    if 'roc_auc_score' in metrics_df.columns:
        plt.plot(metrics_df['threshold_name'], metrics_df['roc_auc_score'], 'o-', label='ROC AUC')
    
    plt.title('Additional Performance Metrics for Different Thresholds')
    plt.xlabel('Threshold')
    plt.ylabel('Score')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'threshold_comparison_additional_metrics.png'))
    plt.close()
    
    plt.figure(figsize=(12, 8))
    plt.bar(metrics_df['threshold_name'], metrics_df['total_true_chunks'], alpha=0.5, label='True Chunks')
    plt.bar(metrics_df['threshold_name'], metrics_df['total_detected_chunks'], alpha=0.5, label='Detected Chunks')
    
    plt.title('Number of Chunks for Different Thresholds')
    plt.xlabel('Threshold')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'threshold_comparison_counts.png'))
    plt.close()
    
    plt.figure(figsize=(12, 8))
    plt.bar(metrics_df['threshold_name'], metrics_df['total_true_positives'], alpha=0.5, label='True Positives')
    plt.bar(metrics_df['threshold_name'], metrics_df['total_false_positives'], alpha=0.5, label='False Positives')
    plt.bar(metrics_df['threshold_name'], metrics_df['total_false_negatives'], alpha=0.5, label='False Negatives')
    
    plt.title('Classification Results for Different Thresholds')
    plt.xlabel('Threshold')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'threshold_comparison_tp_fp_fn.png'))
    plt.close()

def run_test():
    """
    Main function to run the offline autoencoder test
    """
    print("Starting offline autoencoder test...")
    print(os.getcwd())
    
    # Path to model
    file_path = 'models/ae_offline_model_JITC.h5'

    try:
        model = tf.keras.models.load_model(file_path, custom_objects={'mse': MeanSquaredError()})
        print("Model loaded successfully")
        print(model.summary())
    except Exception as e:
        print(f"Error loading model: {e}")
        raise

    # Path to test data and anomalies
    test_path = 'datasets/UA_JITC_test_Bits_Clustered_Dataframe.pkl'
    anomalies_path = 'datasets/UA_JITC_anomalies.pkl'

    try:
        with open(test_path, 'rb') as file:
            test_data = pickle.load(file)
        print('************ Test Data ************')
        print(f"Shape: {test_data.shape}")
        print("Sample data:")
        print(test_data.head())
    except Exception as e:
        print(f"Error loading test data: {e}")
        raise

    try:
        with open(anomalies_path, 'rb') as file:
            ground_truth_anomalies = pickle.load(file)
        print('************ Ground Truth Anomalies ************')
        print(f"Shape: {ground_truth_anomalies.shape}")
        print("Sample data:")
        print(ground_truth_anomalies.head())
    except Exception as e:
        print(f"Error loading ground truth anomalies: {e}")
        raise

    print("************ Debug Ground Truth Anomalies ************")
    for i, (filename, row) in enumerate(ground_truth_anomalies.iterrows()):
        if i >= 5:  # Only show first 5
            break
        print(f"\nFile: {filename}")
        print(f"  Type of anomaly_positions: {type(row['anomaly_positions'])}")
        print(f"  Raw value: {row['anomaly_positions']}")
        
        parsed = parse_anomaly_positions(row['anomaly_positions'])
        print(f"  Parsed positions (first 10): {parsed[:10]}")
        
        chunks = map_bit_positions_to_chunks(parsed)
        print(f"  Mapped to chunks: {sorted(chunks)[:10]}")

    reconstruction_errors = {}
    predictions = {}
    runtimes = []

    print("************ Enter Mode ************")
    # Select mode: 'unit_test' (100 items) or 'none' (full dataset)
    mode = input("Enter mode ('unit_test' or nothing to run full test): ").strip().lower()

    if mode == 'unit_test':
        print("Running in unit test mode with 100 items...")
        
        # Check if the unit test sample contains files with anomalies
        test_files = set(test_data['filename'].unique())
        anomaly_files = set(ground_truth_anomalies.index)
        common_files = test_files.intersection(anomaly_files)
        
        print(f"Files in test set: {len(test_files)}")
        print(f"Files with anomalies: {len(anomaly_files)}")
        print(f"Files with anomalies in test set: {len(common_files)}")
        
        if len(common_files) < 5:
            print("WARNING: Very few files with anomalies in the test set!")
            print("Selecting files that are known to have anomalies...")
            
            files_with_anomalies = []
            for filename, row in ground_truth_anomalies.iterrows():
                parsed = parse_anomaly_positions(row['anomaly_positions'])
                if len(parsed) > 0:
                    files_with_anomalies.append(filename)
            
            selected_files = files_with_anomalies[:50] + list(test_files)[:50]
            test_data = test_data[test_data['filename'].isin(selected_files)]
            print(f"Using {len(test_data)} selected files for testing")
        else:
            test_data = test_data[:100]  # Use only 100 rows for unit testing
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

    print("Processing test data...")
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_row, row): row for _, row in test_data.iterrows()}
        for future in as_completed(futures):
            try:
                filename, prediction, error, runtime = future.result()
                runtimes.append(runtime)

                if filename not in reconstruction_errors:
                    reconstruction_errors[filename] = []
                    predictions[filename] = []
                reconstruction_errors[filename].append(error)
                predictions[filename].append(prediction)
            except Exception as e:
                print(f"Error processing row: {e}")

    all_errors = [error for errors in reconstruction_errors.values() for error in errors]
    print("\nReconstruction Error Statistics:")
    print(f"  Min: {min(all_errors):.6f}")
    print(f"  Max: {max(all_errors):.6f}")
    print(f"  Mean: {np.mean(all_errors):.6f}")
    print(f"  Std: {np.std(all_errors):.6f}")

    # Run evaluation with multiple thresholds
    output_dir = f"UA_JITC_results_{'unit_test' if mode == 'unit_test' else 'full'}"
    metrics = run_batch_evaluation(reconstruction_errors, ground_truth_anomalies, output_dir)

    # Runtime analysis
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
        print(f"{key.capitalize()}: {value:.6f} seconds")

    plt.figure(figsize=(10, 6))
    plt.boxplot(runtimes, vert=False, patch_artist=True)
    plt.title('Boxplot of Runtimes')
    plt.xlabel('Runtime (seconds)')
    plt.savefig(os.path.join(output_dir, 'plots', 'UA_JITC_runtime_boxplot.png'))
    plt.close()

    print(f"\nAll results saved to {output_dir} directory.")
    print("Offline autoencoder unit test complete!")

if __name__ == "__main__":
    run_test()