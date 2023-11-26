#!/usr/bin/env python 

"""
Application:        PySpark Pipeline for Streaming DIS and utilizing EVL
File name:          evlStream.py 
Author:             Martin Manuel Lopez
Creation:           11/23/2023

The University of Arizona
Department of Electrical and Computer Engineering
College of Engineering
"""

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import numpy as np
import pandas as pd
from tqdm import tqdm
from pyspark.mllib.clustering import StreamingKMeans, StreamingKMeansModel
from pyspark.ml.clustering import GaussianMixture as GMM
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from keras.preprocessing.sequence import pad_sequences
# from keras_preprocessing.sequence import pad_sequences
from pyspark.ml.classification import LinearSVC,
import models.evl.classifier_performance as cp
from scipy import stats
from sklearn.metrics import silhouette_score
import time 
from matplotlib import pyplot as plt
from matplotlib import patches as mpatches
from matplotlib.axes._axes import _log as matplotlib_axes_logger
matplotlib_axes_logger.setLevel('ERROR')

class MClassificationStream:
    def __init__(self,
                 classifier,
                 method,
                 dataset,
                 datasource,
                 graph = True):
        
        self.classifier = classifier
        self.dataset = dataset
        self.datasource = datasource
        self.NClusters = 0
        self.method = method
        self.cluster_centers ={}
        self.graph = graph
        self.preds = {}
        self.class_cluster = {}
        self.clusters = {}
        self.total_time = []
        self.performance_metric = {}
        self.avg_perf_metric = {}
        self.microCluster = {}
        self.X = {}
        self.Y = {}
        self.T = {}
        # for UNSW IoT dataset
        self.data= {}
        self.labeled= {}
        self.Xinit = {}
        self.Yinit = {}
        self.all_data = {}
        self.all_data_test = {}

    def find_silhoette_score(self, X, y): 
        sil_score = {}
        for c in range(2,11):
            kmeans_model = StreamingKMeans(k=c, decayFactor=1.0).setRandomCenters(3, 1.0, 0).train(X)
            score = silhouette_score(X, kmeans_model.labels_, metric='euclidean')
            sil_score[c] = score
        optimal_cluster = max(sil_score, key=sil_score.get)
        self.NClusters = optimal_cluster

    def cluster(self, X, y):
        # Need to determine what happens in the first iteration
        self.find_silhoette_score(X, y)
        kmeans_model = StreamingKMeans(k=self.NClusters, decayFactor=1.0).trainOn(X)
        self.microCluster

    def initLabelData(self, inData, inLabels):
        self.cluster()
