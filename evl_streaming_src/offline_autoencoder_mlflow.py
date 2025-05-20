#!/usr/bin/env python

import os
import pickle
import time
import datetime
import numpy as np
import pandas as pd
import tensorflow as tf
import mlflow
import argparse
import json
from joblib import load as joblib_load
from tensorflow.keras.losses import MeanSquaredError
from sklearn.metrics import precision_score, recall_score, f1_score
from performance_results import PerformanceMetrics

# ------------------------
# Utility Functions
# ------------------------
def log_metrics_to_mlflow(metrics: dict):
    for k, v in metrics.items():
        if isinstance(v, (int, float)):
            mlflow.log_metric(k, v)
        elif isinstance(v, str):
            # Only log param if it doesn't already exist
            try:
                mlflow.log_param(k, v)
            except mlflow.exceptions.MlflowException as e:
                if "Param with key" in str(e) and "was already logged" in str(e):
                    pass  # Ignore param conflict
                else:
                    raise

def parse_anomaly_positions(pos):
    if isinstance(pos, str):
        try:
            return list(map(int, json.loads(pos))) if pos.startswith("[") else list(map(int, pos.split(',')))
        except:
            return [int(pos)] if pos.isdigit() else []
    return list(map(int, pos)) if isinstance(pos, list) else []

def map_to_chunks(positions, chunk_size=256):
    return sorted({int(p) // chunk_size for p in positions})

def compute_binary_arrays(true_chunks, detected_chunks):
    max_chunk = max(max(true_chunks, default=0), max(detected_chunks, default=0)) + 1
    true_bin = [1 if i in true_chunks else 0 for i in range(max_chunk)]
    pred_bin = [1 if i in detected_chunks else 0 for i in range(max_chunk)]
    return true_bin, pred_bin

def compute_threshold_metrics(true_bin, pred_bin, name, threshold):
    metrics = {
        'threshold_name': name,
        'threshold_value': threshold,
        'precision': precision_score(true_bin, pred_bin, zero_division=0),
        'recall': recall_score(true_bin, pred_bin, zero_division=0),
        'f1_score': f1_score(true_bin, pred_bin, zero_division=0)
    }
    pm = PerformanceMetrics(pred_bin, true_bin, dataset="JITC", method=f"Autoencoder-{name}", classifier="ae")
    extra = pm.findClassifierMetrics(pred_bin, true_bin)
    metrics.update({
        'classifier_accuracy': extra['Classifier_Accuracy'],
        'classifier_error': extra['Classifier_Error'],
        'matthews_corrcoef': extra['Matthews_CorrCoef'],
        'roc_auc_score': extra['ROC_AUC_Score'] if extra['ROC_AUC_Score'] != 'Only one class found' else 0
    })
    return metrics

def process_test_data(model, df, scaler):
    grouped = df.groupby('filename')
    errors = {}
    for fname, group in grouped:
        errs = []
        for val in group['bit_number_scaled']:
            x = np.array([[val]], dtype=np.float32)
            x = scaler.transform(x)
            pred = model.predict(x, verbose=0)
            errs.append(np.mean(np.square(x - pred)))
        errors[fname] = errs
    return errors

def get_thresholds(errors_dict):
    all_errors = [e for lst in errors_dict.values() for e in lst]
    percentiles = [99, 95, 90, 85, 80, 75, 70]
    return {f'percentile_{p}': np.percentile(all_errors, p) for p in percentiles}

def evaluate_threshold(thresh_name, thresh_val, errors, ground_truth):
    all_true, all_pred = [], []
    label_true, label_pred = [], []

    for fname, row in ground_truth.iterrows():
        if fname not in errors: continue
        true_chunks = map_to_chunks(parse_anomaly_positions(row['anomaly_positions']))
        pred_chunks = [i for i, err in enumerate(errors[fname]) if err > thresh_val]
        tb, pb = compute_binary_arrays(true_chunks, pred_chunks)
        all_true.extend(tb)
        all_pred.extend(pb)

        # If ground_truth has label column, evaluate label-wise classification metrics
        if 'labels' in row:
            true_label = int(row['labels'])
            pred_label = 1 if len(pred_chunks) > 0 else 0
            label_true.append(true_label)
            label_pred.append(pred_label)

    metrics = compute_threshold_metrics(all_true, all_pred, thresh_name, thresh_val)

    # If we also collected label-wise predictions, add those metrics
    if label_true and label_pred:
        pm_labels = PerformanceMetrics(label_pred, label_true, dataset="JITC", method=f"LabelEval-{thresh_name}", classifier="ae")
        label_metrics = pm_labels.findClassifierMetrics(label_pred, label_true)
        metrics.update({
            'label_classifier_accuracy': label_metrics['Classifier_Accuracy'],
            'label_classifier_error': label_metrics['Classifier_Error'],
            'label_roc_auc_score': label_metrics['ROC_AUC_Score'] if label_metrics['ROC_AUC_Score'] != 'Only one class found' else 0,
            'label_matthews_corrcoef': label_metrics['Matthews_CorrCoef'],
            'label_f1_score': label_metrics['F1_Score']
        })

    log_metrics_to_mlflow(metrics)
    return metrics

def main(args):
    output_dir = "jitc_eval_results"
    os.makedirs(output_dir, exist_ok=True)
    results_collection = []
    output_dir = "jitc_eval_results"
    os.makedirs(output_dir, exist_ok=True)
    results_collection = []

    mlflow.set_tracking_uri("mlruns")
    mlflow.set_experiment("TCIS-Autoencoder-Anomaly-Detection")
    with mlflow.start_run(run_name=f"autoencoder-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"):
        model = tf.keras.models.load_model(args.model_path, custom_objects={'mse': MeanSquaredError()})
        scaler = joblib_load(args.scaler_path)

        with open(args.test_data, 'rb') as f:
            test_df = pickle.load(f)
        with open(args.anomalies, 'rb') as f:
            anomalies = pickle.load(f)

        errors = process_test_data(model, test_df, scaler)

        if args.threshold_file:
            df_thresh = pd.read_csv(args.threshold_file)
            threshold_val = df_thresh['Stream_threshold'].iloc[0]
            print(f"Evaluating using provided threshold: {threshold_val}")
            metrics = evaluate_threshold("stream_threshold", threshold_val, errors, anomalies)
            results_collection.append(metrics)
        else:
            thresholds = get_thresholds(errors)
            for name, value in thresholds.items():
                with mlflow.start_run(run_name=f"threshold-{name}", nested=True):
                    metrics = evaluate_threshold(name, value, errors, anomalies)
                    results_collection.append(metrics)
                    results_collection.append(metrics)

# Save metrics to a pickle and parquet file
    results_df = pd.DataFrame(results_collection)
    results_df.to_pickle(os.path.join(output_dir, "threshold_evaluation_results.pkl"))
    results_df.to_parquet(os.path.join(output_dir, "threshold_evaluation_results.parquet"), index=False)

    print(f"Evaluation results saved to {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_path', default='models/ae_offline_model_JITC.h5')
    parser.add_argument('--scaler_path', default='models/ae_offline_scaler_JITC.pkl')
    parser.add_argument('--test_data', default='datasets/UA_JITC_test_Bits_Clustered_Dataframe.pkl')
    parser.add_argument('--anomalies', default='datasets/UA_JITC_anomalies.pkl')
    parser.add_argument('--threshold_file', default='', help='CSV file with Stream_threshold column (optional)')
    args = parser.parse_args()
    main(args)
    print("Script completed successfully.")
