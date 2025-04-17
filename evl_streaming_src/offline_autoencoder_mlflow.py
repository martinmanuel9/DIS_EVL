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
import mlflow
import datetime
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from keras.losses import MeanSquaredError
from sklearn.metrics import precision_score, recall_score, f1_score
from sklearn.exceptions import UndefinedMetricWarning
from performance_result_no_ts import PerformanceMetrics as pm

# MLflow setup
MLFLOW_TRACKING_URI = "mlruns"  # Local directory for MLflow tracking
EXPERIMENT_NAME = "TCIS-Autoencoder-Anomaly-Detection"

def setup_mlflow():
    """
    Set up MLflow tracking
    === PORT NOTE ===
    If MLflow UI is already using port 5000, run this instead:
        mlflow ui --port 5050
    And access it at http://localhost:5050
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    try:
        experiment_id = mlflow.create_experiment(
            EXPERIMENT_NAME,
            artifact_location=os.path.join(os.getcwd(), MLFLOW_TRACKING_URI, EXPERIMENT_NAME)
        )
    except mlflow.exceptions.MlflowException:
        experiment_id = mlflow.get_experiment_by_name(EXPERIMENT_NAME).experiment_id

    mlflow.set_experiment(EXPERIMENT_NAME)
    return experiment_id

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
                print([int(pos) for pos in anomaly_positions.split(',')])
                return [int(pos) for pos in json.loads(anomaly_positions)]
            except json.JSONDecodeError:
                print(f"Error parsing JSON array: {anomaly_positions}")
                return []
        elif ',' in anomaly_positions:
            # Comma-separated string
            print([int(pos) for pos in anomaly_positions.split(',')])
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
    
    # Save figure for MLflow
    plot_path = os.path.join(output_dir, 'error_distribution_all_thresholds.png')
    plt.savefig(plot_path)
    mlflow.log_artifact(plot_path)
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
        plot_path = os.path.join(output_dir, f'error_distribution_{name}.png')
        plt.savefig(plot_path)
        mlflow.log_artifact(plot_path)
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
        
        # Using PerformanceMetrics class to calculate additional metrics
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
        
        # Log metrics to MLflow
        with mlflow.start_run(nested=True, run_name=f"threshold-{threshold_name}"):
            mlflow.log_param("threshold_name", threshold_name)
            mlflow.log_param("threshold_value", threshold)
            mlflow.log_metric("precision", metrics['precision'])
            mlflow.log_metric("recall", metrics['recall'])
            mlflow.log_metric("f1_score", metrics['f1_score'])
            mlflow.log_metric("matthews_corrcoef", metrics['matthews_corrcoef'])
            mlflow.log_metric("classifier_accuracy", metrics['classifier_accuracy'])
            mlflow.log_metric("total_files", metrics['total_files'])
            mlflow.log_metric("total_true_chunks", metrics['total_true_chunks'])
            mlflow.log_metric("total_detected_chunks", metrics['total_detected_chunks'])
            mlflow.log_metric("total_true_positives", metrics['total_true_positives'])
            mlflow.log_metric("total_false_positives", metrics['total_false_positives'])
            mlflow.log_metric("total_false_negatives", metrics['total_false_negatives'])
            
            if 'roc_auc_score' in metrics:
                mlflow.log_metric("roc_auc_score", metrics['roc_auc_score'])
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
        
        # Save individual results to CSV
        file_results_df = pd.DataFrame(results['file_results'])
        file_results_path = os.path.join(output_dir, f'file_results_{threshold_name}.csv')
        file_results_df.to_csv(file_results_path, index=False)
        
        # Log to MLflow
        mlflow.log_artifact(file_results_path)
    
    # Save combined results
    metrics_df = pd.DataFrame(all_metrics)
    file_results_df = pd.DataFrame(all_file_results)
    
    metrics_path = os.path.join(output_dir, 'threshold_metrics_summary.csv')
    all_results_path = os.path.join(output_dir, 'all_file_results.csv')
    
    metrics_df.to_csv(metrics_path, index=False)
    file_results_df.to_csv(all_results_path, index=False)
    
    # Save results to pickle file
    results_data = {
        'metrics': all_metrics,
        'file_results': all_file_results,
        'thresholds': thresholds,
        'reconstruction_errors': reconstruction_errors
    }
    
    # Save results to pickle file
    results_pkl_path = os.path.join(output_dir, 'evaluation_results.pkl')
    with open(results_pkl_path, 'wb') as f:
        pickle.dump(results_data, f)
    
    # Log artifacts to MLflow
    mlflow.log_artifact(metrics_path)
    mlflow.log_artifact(all_results_path)
    mlflow.log_artifact(results_pkl_path)
    
    # Generate and save plots
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
    
    plot_path = os.path.join(output_dir, 'threshold_comparison_metrics.png')
    plt.savefig(plot_path)
    mlflow.log_artifact(plot_path)
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
    
    plot_path = os.path.join(output_dir, 'threshold_comparison_additional_metrics.png')
    plt.savefig(plot_path)
    mlflow.log_artifact(plot_path)
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
    
    plot_path = os.path.join(output_dir, 'threshold_comparison_counts.png')
    plt.savefig(plot_path)
    mlflow.log_artifact(plot_path)
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
    
    plot_path = os.path.join(output_dir, 'threshold_comparison_tp_fp_fn.png')
    plt.savefig(plot_path)
    mlflow.log_artifact(plot_path)
    plt.close()

    # Create F1 score vs threshold plot
    plt.figure(figsize=(10, 6))
    plt.plot(metrics_df['threshold_value'], metrics_df['f1_score'], 'o-')
    plt.title('F1 Score vs Threshold Value')
    plt.xlabel('Threshold Value')
    plt.ylabel('F1 Score')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    plot_path = os.path.join(output_dir, 'f1_vs_threshold.png')
    plt.savefig(plot_path)
    mlflow.log_artifact(plot_path)
    plt.close()

def find_optimal_threshold(metrics_list):
    """
    Find the threshold with the best F1 score
    """
    metrics_df = pd.DataFrame(metrics_list)
    best_idx = metrics_df['f1_score'].idxmax()
    best_threshold = metrics_df.loc[best_idx]
    
    print("\nOptimal Threshold:")
    print(f"  Name: {best_threshold['threshold_name']}")
    print(f"  Value: {best_threshold['threshold_value']:.6f}")
    print(f"  F1 Score: {best_threshold['f1_score']:.4f}")
    print(f"  Precision: {best_threshold['precision']:.4f}")
    print(f"  Recall: {best_threshold['recall']:.4f}")
    
    return best_threshold

def run_inference(model, input_data, threshold):
    """
    Run inference on new data to predict anomalies
    """
    predictions = []
    anomalies = []
    
    max_bit_number = input_data['bit_number'].max()
    
    for _, row in input_data.iterrows():
        bit_number_scaled = float(row['bit_number_scaled'])
        X_row = np.array([bit_number_scaled], dtype=np.float32)
        X_row = np.expand_dims(X_row, axis=0)
        
        prediction = model.predict(X_row, verbose=0)
        error = np.mean(np.square(X_row - prediction))
        
        predictions.append(prediction.flatten())
        anomalies.append(error > threshold)
    
    input_data['prediction'] = predictions
    input_data['error'] = [np.mean(np.square(row['bit_number_scaled'] - pred)) for row, pred in zip(input_data.iterrows(), predictions)]
    input_data['is_anomaly'] = anomalies
    
    return input_data

def process_row(row, model, max_bit_number):
    start_time = time.time()
    bit_number = float(row['bit_number']) / max_bit_number
    bit_number_scaled = float(row['bit_number_scaled'])
    
    X_row = np.array([bit_number_scaled], dtype=np.float32)
    X_row = np.expand_dims(X_row, axis=0)  
    prediction = model.predict(X_row, verbose=0)
    error = np.mean(np.square(X_row - prediction))
    end_time = time.time()
    
    return row['filename'], prediction.flatten(), error, end_time - start_time

def run_test():
    """
    Main function to run the offline autoencoder unit test
    """
    print("Starting offline autoencoder...")
    
    # Setup MLflow
    experiment_id = setup_mlflow()
    
    # Get current timestamp for run name
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_name = f"autoencoder-run-{timestamp}"
    
    # Start MLflow run
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name):
        # Path to model
        print(os.getcwd())
        file_path = 'evl_streaming_src/models/ae_offline_model_JITC.h5'

        try:
            model = tf.keras.models.load_model(file_path, custom_objects={'mse': MeanSquaredError()})
            print("Model loaded successfully")
            print(model.summary())
            
            # Log model to MLflow
            mlflow.tensorflow.log_model(model, "model")
        except Exception as e:
            print(f"Error loading model: {e}")
            raise

        # Path to test data and anomalies
        test_path = 'evl_streaming_src/datasets/UA_JITC_test_Bits_Clustered_Dataframe.pkl'
        anomalies_path = 'evl_streaming_src/datasets/UA_JITC_anomalies.pkl'

        try:
            with open(test_path, 'rb') as file:
                test_data = pickle.load(file)
            print('************ Test Data ************')
            print(f"Shape: {test_data.shape}")
            print("Sample data:")
            print(test_data.head())
            
            # Log test data info
            mlflow.log_param("test_data_shape", str(test_data.shape))
            mlflow.log_param("test_data_columns", str(test_data.columns.tolist()))
        except Exception as e:
            print(f"Error loading test data: {e}")
            raise

        try:
            with open(anomalies_path, 'rb') as file:
                ground_truth_anomalies = pickle.load(file)
            print('************ Ground Truth Anomalies ************')
            print(f"Shape: {ground_truth_anomalies.shape}")
            print("Sample ground truth data:")
            print(ground_truth_anomalies.head())
            
            # Log anomalies info
            mlflow.log_param("anomalies_shape", str(ground_truth_anomalies.shape))
        except Exception as e:
            print(f"Error loading ground truth anomalies: {e}")
            raise

        print("\n************ Debug Ground Truth Anomalies ************")
        for i, (filename, row) in enumerate(ground_truth_anomalies.iterrows()):
            if i >= 5:  # Only show first 5
                break
            print(f"\nFile: {ground_truth_anomalies.values[i][0]}")
            print(f"  Type of anomaly_positions: {type(row['anomaly_positions'])}")
            print(f"  Truth value: {row['anomaly_positions']}")
            
            parsed = parse_anomaly_positions(row['anomaly_positions'])
            print(f"  Parsed positions (first 10): {parsed[:10]}")
            
            chunks = map_bit_positions_to_chunks(parsed)
            print(f"  Mapped to chunks: {sorted(chunks)[:10]}")

        reconstruction_errors = {}
        predictions = {}
        runtimes = []

        print("\n************ Enter Mode ************")
        # Select mode: 'unit_test' (100 items) or 'none' (full dataset)
        mode = input("Enter mode ('unit_test' or nothing to run full test): ").strip().lower()
        
        # Log mode to MLflow
        mlflow.log_param("test_mode", mode if mode else "full")

        if mode == 'unit_test':
            print("Running in unit test mode with 100 items...")
            
            # Check if the unit test sample contains files with anomalies
            # TODO the real test and performance is found in ground_truth but right now
            # we are just getting the file names
            # what we need to do is correct file names accross the test set and put together and understand if 
            # there is an anomaly. 
            test_files = set(test_data['filename'].unique())
            anomaly_files = set(ground_truth_anomalies.values[:, 0])
            common_files = test_files.intersection(anomaly_files)
            
            print(f"Files in test set: {len(test_files)}")
            print(f"Files with anomalies: {len(anomaly_files)}")
            print(f"Files with anomalies in test set: {len(common_files)}")
            
            # Log files info
            mlflow.log_param("test_files_count", len(test_files))
            mlflow.log_param("anomaly_files_count", len(anomaly_files))
            mlflow.log_param("common_files_count", len(common_files))
            
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
                
                # Log selection info
                mlflow.log_param("selected_files_count", len(test_data))
            else:
                test_data = test_data[:100]  # Use only 100 rows for unit testing
                
                # Log selection info
                mlflow.log_param("selected_rows_count", len(test_data))
        else:
            print("Running full simulation...")
            
            # Log full test info
            mlflow.log_param("full_test_rows_count", len(test_data))

        max_bit_number = test_data['bit_number'].max()
        
        # Log max bit number
        mlflow.log_param("max_bit_number", max_bit_number)

        # Log start time
        processing_start_time = time.time()
        
        print("Processing test data...")
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(process_row, row, model, max_bit_number): row for _, row in test_data.iterrows()}
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
        
        print(test_data.head())
        perf_metric = pm.PerformanceMetrics(preds= predictions, test= test_data, \
                                        dataset= 'JITC_AE', method= 'Autoencoder' , \
                                        classifier= 'TCIS_Autoencoder')
        performance_metric = perf_metric.findClassifierMetrics(preds= self.preds[ts], test= self.X[ts+1][:,-1])
        # Log processing end time
        processing_end_time = time.time()
        total_processing_time = processing_end_time - processing_start_time
        
        # Log processing time to MLflow
        mlflow.log_metric("total_processing_time", total_processing_time)
        mlflow.log_metric("avg_processing_time_per_row", total_processing_time / len(test_data))

        all_errors = [error for errors in reconstruction_errors.values() for error in errors]
        print("\nReconstruction Error Statistics:")
        print(f"  Min: {min(all_errors):.6f}")
        print(f"  Max: {max(all_errors):.6f}")
        print(f"  Mean: {np.mean(all_errors):.6f}")
        print(f"  Std: {np.std(all_errors):.6f}")
        
        # Log error statistics to MLflow
        mlflow.log_metric("min_error", min(all_errors))
        mlflow.log_metric("max_error", max(all_errors))
        mlflow.log_metric("mean_error", np.mean(all_errors))
        mlflow.log_metric("std_error", np.std(all_errors))

        # Run evaluation with multiple thresholds
        output_dir = f"UA_JITC_results_{'unit_test' if mode == 'unit_test' else 'full'}"
        metrics = run_batch_evaluation(reconstruction_errors, ground_truth_anomalies, output_dir)
        
        # Find the optimal threshold
        best_threshold = find_optimal_threshold(metrics)
        
        # Log best threshold to MLflow
        mlflow.log_param("best_threshold_name", best_threshold['threshold_name'])
        mlflow.log_param("best_threshold_value", best_threshold['threshold_value'])
        mlflow.log_metric("best_f1_score", best_threshold['f1_score'])
        mlflow.log_metric("best_precision", best_threshold['precision'])
        mlflow.log_metric("best_recall", best_threshold['recall'])

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
            # Log runtime stats to MLflow
            mlflow.log_metric(f"runtime_{key}", value)

        plt.figure(figsize=(10, 6))
        plt.boxplot(runtimes, vert=False, patch_artist=True)
        plt.title('Boxplot of Runtimes')
        plt.xlabel('Runtime (seconds)')
        runtime_plot_path = os.path.join(output_dir, 'plots', 'UA_JITC_runtime_boxplot.png')
        plt.savefig(runtime_plot_path)
        mlflow.log_artifact(runtime_plot_path)
        plt.close()

        # Save all reconstruction errors and predictions
        error_data = {
            'reconstruction_errors': reconstruction_errors,
            'predictions': predictions,
            'runtime_summary': runtime_summary
        }
        
        error_data_path = os.path.join(output_dir, 'reconstruction_errors.pkl')
        with open(error_data_path, 'wb') as f:
            pickle.dump(error_data, f)
        
        # Log error data to MLflow
        mlflow.log_artifact(error_data_path)
        
        # Create a model inference function with the best threshold
#         model_inference_script = f"""
# import pickle
# import numpy as np
# import tensorflow as tf
# from tensorflow.keras.losses import MeanSquaredError

# def load_model(model_path):
#     return tf.keras.models.load_model(model_path, custom_objects={{'mse': MeanSquaredError()}})

# def predict_anomalies(model, data, threshold={best_threshold['threshold_value']}):
#     \"\"\"
#     Predict anomalies in the input data using the autoencoder model
    
#     Args:
#         model: Trained autoencoder model
#         data: Input data with 'bit_number_scaled' column
#         threshold: Threshold for anomaly detection ({best_threshold['threshold_name']})
        
#     Returns:
#         Data with predictions and anomaly flags
#     \"\"\"
#     predictions = []
#     errors = []
#     anomalies = []
    
#     for _, row in data.iterrows():
#         bit_number_scaled = float(row['bit_number_scaled'])
#         X_row = np.array([bit_number_scaled], dtype=np.float32)
#         X_row = np.expand_dims(X_row, axis=0)
        
#         prediction = model.predict(X_row, verbose=0)
#         error = np.mean(np.square(X_row - prediction))
        
#         predictions.append(prediction.flatten()[0])
#         errors.append(error)
#         anomalies.append(error > threshold)
    
#     # Add predictions to data
#     data['prediction'] = predictions
#     data['error'] = errors
#     data['is_anomaly'] = anomalies
    
#     return data

# # Example usage:
# model = load_model('evl_streaming_src/models/ae_offline_model_JITC.h5')
# input_data = 'evl_streaming_src/datasets/UA_JITC_test_Bits_Clustered_Dataframe.pkl'
# result = predict_anomalies(model, input_data)
# """
        
        # Save inference script
        # inference_script_path = os.path.join(output_dir, 'model_inference.py')
        # with open(inference_script_path, 'w') as f:
        #     f.write(model_inference_script)
        
        # # Log inference script to MLflow
        # mlflow.log_artifact(inference_script_path)

        print(f"\nAll results saved to {output_dir} directory.")
        print("Offline autoencoder test complete!")
        
        # Create a markdown report with results summary
        report = f"""# Autoencoder Anomaly Detection Results
## Test Configuration
- Test Mode: {'Unit Test' if mode == 'unit_test' else 'Full Test'}
- Files Analyzed: {len(reconstruction_errors)}
- Data Points: {sum(len(errors) for errors in reconstruction_errors.values())}

## Performance Metrics
- Best Threshold: {best_threshold['threshold_name']} ({best_threshold['threshold_value']:.6f})
- F1 Score: {best_threshold['f1_score']:.4f}
- Precision: {best_threshold['precision']:.4f}
- Recall: {best_threshold['recall']:.4f}
- Matthews Correlation: {best_threshold['matthews_corrcoef']:.4f}

## Error Statistics
- Min Error: {min(all_errors):.6f}
- Max Error: {max(all_errors):.6f}
- Mean Error: {np.mean(all_errors):.6f}
- Std Error: {np.std(all_errors):.6f}

## Runtime Performance
- Mean Runtime: {runtime_summary['mean']:.6f} seconds
- Median Runtime: {runtime_summary['median']:.6f} seconds
- Std Runtime: {runtime_summary['standard_deviation']:.6f} seconds

## Files
- Model: {file_path}
- Test Data: {test_path}
- Ground Truth: {anomalies_path}
- Results: {output_dir}

## Next Steps
1. Use the generated 'model_inference.py' script for real-time anomaly detection
2. Deploy the model with the optimal threshold ({best_threshold['threshold_value']:.6f})
3. Monitor performance on new data
"""
        
        # Save report
        report_path = os.path.join(output_dir, 'results_summary.md')
        with open(report_path, 'w') as f:
            f.write(report)
        
        # Log report to MLflow
        mlflow.log_artifact(report_path)
        
if __name__ == "__main__":
    run_test()