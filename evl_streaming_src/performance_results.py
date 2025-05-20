#!/usr/bin/env python

"""
Application:        Performance Metrics for Anomaly Detection
File name:          performance_metrics.py
Author:             Martin Manuel Lopez
Modified by:        [Your Name]
Creation:           April 16, 2025
Modification:       April 28, 2025

The University of Arizona
Department of Electrical and Computer Engineering
College of Engineering
"""

import numpy as np
import pandas as pd
import sklearn.metrics as metric

class PerformanceMetrics:
    """
    Intent of Classification Class is to get the classification metrics for either 
    a single timestep or an overtime performance of the a classifier 
    Initialization requires:
        preds = predictions from classifier 
        test = expected results 
        dataset = what is the dataset you are running 
        method = the algorithm running (e.g., Autoencoder, COMPOSE, etc.)
        classifier = classifying algorithm
    """
    def __init__(self, preds=None, test=None, dataset=None, method=None, classifier=None):
        self.preds = preds
        self.test = test
        self.selected_dataset = dataset
        self.method = method
        self.classifier = classifier
        # classification metrics
        self.classifier_error = {}
        self.classifier_accuracy = {}
        self.roc_auc_score = {}
        self.roc_auc_plot = {}
        self.f1_score = {}
        self.mathews_corr_coeff = {}
        # results
        self.perf_metrics = {}
        # Default timestep if not provided
        self.ts = 0

    def findClassifierMetrics(self, preds, test):
        """
        Calculate classification metrics between predictions and ground truth
        
        Args:
            preds: Predicted labels (0 for normal, 1 for anomaly)
            test: True labels (0 for normal, 1 for anomaly)
            
        Returns:
            Dictionary of performance metrics
        """
        # Convert inputs to numpy arrays if they aren't already
        if not isinstance(preds, np.ndarray):
            preds = np.array(preds)
        if not isinstance(test, np.ndarray):
            test = np.array(test)
            
        # Handle 0-dimensional arrays
        if preds.ndim == 0:
            preds = np.array([preds])
        if test.ndim == 0:
            test = np.array([test])
            
        # Calculate classifier error and accuracy
        with np.errstate(divide='ignore', invalid='ignore'):
            self.classifier_error[self.ts] = np.sum(preds != test) / len(preds) if len(preds) > 0 else 0
        
        self.classifier_accuracy[self.ts] = 1 - self.classifier_error[self.ts]
        
        # Calculate ROC AUC score if possible
        try:
            self.roc_auc_score[self.ts] = metric.roc_auc_score(test, preds)
            fpr, tpr, _ = metric.roc_curve(test, preds, pos_label=1)
            self.roc_auc_plot[self.ts] = [fpr, tpr]
        except ValueError:
            self.roc_auc_score[self.ts] = 'Only one class found'
            self.roc_auc_plot[self.ts] = 'Only one class found'
        
        # Calculate F1 score
        self.f1_score[self.ts] = metric.f1_score(test.astype(int), preds.astype(int), average=None) 
        
        # Calculate Matthews Correlation Coefficient
        self.mathews_corr_coeff[self.ts] = metric.matthews_corrcoef(test.astype(int), preds.astype(int))
        
        # Populate results dictionary
        self.perf_metrics = {
            'Dataset': self.selected_dataset,
            'Classifier': self.classifier,
            'Method': self.method,
            'Classifier_Error': self.classifier_error[self.ts],
            'Classifier_Accuracy': self.classifier_accuracy[self.ts],
            'ROC_AUC_Score': self.roc_auc_score[self.ts],
            'ROC_AUC_Plotter': self.roc_auc_plot[self.ts],
            'F1_Score': self.f1_score[self.ts],
            'Matthews_CorrCoef': self.mathews_corr_coeff[self.ts]
        }
        
        return self.perf_metrics

    def findAvgPerfMetrics(self, metrics_dict):
        """
        Calculate average performance metrics across multiple time steps
        
        Args:
            metrics_dict: Dictionary containing metrics at different time steps
            
        Returns:
            Dictionary of average performance metrics
        """
        avg_results = {}
        
        # Extract dataset, classifier, and method from the first metrics entry
        first_key = list(metrics_dict.keys())[0]
        avg_results['Dataset'] = metrics_dict[first_key]['Dataset'] 
        avg_results['Classifier'] = metrics_dict[first_key]['Classifier']
        avg_results['Method'] = metrics_dict[first_key]['Method']
        
        # Calculate average error and accuracy
        errors = []
        accuracies = []
        roc_scores = []
        f1_scores = []
        matt_coeffs = []
        
        for ts in metrics_dict.keys():
            errors.append(metrics_dict[ts]['Classifier_Error'])
            accuracies.append(metrics_dict[ts]['Classifier_Accuracy'])
            
            if metrics_dict[ts]['ROC_AUC_Score'] != 'Only one class found':
                roc_scores.append(metrics_dict[ts]['ROC_AUC_Score'])
            
            f1_scores.append(metrics_dict[ts]['F1_Score'])
            matt_coeffs.append(metrics_dict[ts]['Matthews_CorrCoef'])
        
        # Calculate averages
        avg_results['Avg_Error'] = np.mean(errors) if errors else 0
        avg_results['Avg_Accuracy'] = np.mean(accuracies) if accuracies else 0
        avg_results['Avg_ROC_AUC_Score'] = np.mean(roc_scores) if roc_scores else 'Only one class found'
        
        # For F1 scores, we need to handle multi-class case appropriately
        if f1_scores:
            first_shape = f1_scores[0].shape
            compatible_scores = [score for score in f1_scores if score.shape == first_shape]
            if compatible_scores:
                avg_results['Avg_F1_Score'] = np.mean(compatible_scores, axis=0)
            else:
                avg_results['Avg_F1_Score'] = np.array([0])
        else:
            avg_results['Avg_F1_Score'] = np.array([0])
        
        avg_results['Avg_Matthews_Corr_Coeff'] = np.mean(matt_coeffs) if matt_coeffs else 0
        
        # Store timesteps and corresponding accuracies for plotting
        avg_results['Timesteps'] = np.array(list(metrics_dict.keys()))
        avg_results['Accuracy'] = np.array(accuracies)
        
        return avg_results