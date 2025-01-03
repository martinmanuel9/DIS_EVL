#%%
#!/usr/bin/env python

"""
Application:        Online Learning in Extreme Verification Latency
File name:          run_experiment.py
Author:             Martin Manuel Lopez
Creation:           07/13/2023

The University of Arizona
Department of Electrical and Computer Engineering
College of Engineering
"""

# MIT License
#
# Copyright (c) 2021 Martin M Lopez
#
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


from matplotlib import pyplot as plt
import pandas as pd
import pickle as pickle 
import time
import scargc
import mclassification
import vanilla
import os
import time
from pathlib import Path 
import multiprocessing

class RunExperiment:
    def __init__(self, experiments=[], classifiers=[], datasets=[], datasources=[], methods=[]):
        self.experiments = experiments
        self.classifiers = classifiers
        self.datasets = datasets
        self.results = {}
        self.datasources = datasources
        self.methods = methods
        
    def change_dir(self):
        # path = str(Path.home())
        # print(path)
        print(os.getcwd())
        path = os.getcwd() + "/evl_streaming_src/results"
        print(path)
        os.chdir(path)
        print(os.getcwd())

    def plot_results(self):
        # change the directory to your particular files location
        self.change_dir()
        # path = str(Path.home())
        path = "../plots"
        os.chdir(path)
        experiments = self.results.keys()
        fig_handle = plt.figure()
        fig, ax = plt.subplots()
        result_plot = {}
        for experiment in experiments:
            result_plot['Timesteps'] = self.results[experiment]['Timesteps']
            result_plot['Accuracy'] = self.results[experiment]['Accuracy']
            df = pd.DataFrame(result_plot)
            df.plot(ax=ax, label=experiment, x='Timesteps', y='Accuracy')
            
        time_stamp = time.strftime("%Y%m%d-%H%M%S")
        plt.title('Accuracy over timesteps' + time_stamp )
        plt.xlabel('Timesteps')
        plt.tight_layout()
        plt.legend
        plt.ylabel('% Accuracy')
        plt.gcf().set_size_inches(15,10)  
        plt.show()
        results_pkl = 'result_plot_data_' + f'{time_stamp}' + '.pkl'
        with open(f'{results_pkl}', 'wb') as result_data:
            pickle.dump(self.results, result_data)

    def createExperiment(self, experiment, classifier, datasource, dataset, method):
        if experiment == 'vanilla':
            experiment = experiment + '_' + dataset + '_' + classifier + '_' + datasource
            van_nb = vanilla.VanillaClassifier(classifier= classifier, dataset= dataset)
            self.results[experiment] = van_nb.run()
            time_stamp = time.strftime("%Y%m%d-%H:%M:%S")
            van_nb.avg_perf_metric['Experiment'] = experiment 
            van_nb.avg_perf_metric['Time_Stamp'] = time_stamp
            results_df = pd.DataFrame.from_dict((van_nb.avg_perf_metric.keys(), van_nb.avg_perf_metric.values())).T
            # change the directory to your particular files location
            self.change_dir()
            results_van_nb = 'results_'+ f'{experiment}' +'.pkl'
            results_df.to_pickle(results_van_nb)
            results_pkl = pd.read_pickle(results_van_nb)
            print("Results:\n" , results_pkl )
        elif experiment == 'scargc':

            experiment = experiment + '_' + dataset + '_' + classifier + '_' + datasource
            scargc_ab = scargc.SCARGC(classifier = classifier, dataset= dataset, datasource= datasource, resample=False) # resample false to remove randomizer
            self.results[experiment] = scargc_ab.run()
            time_stamp = time.strftime("%Y%m%d-%H:%M:%S")
            scargc_ab.avg_perf_metric['Experiment'] = experiment 
            scargc_ab.avg_perf_metric['Time_Stamp'] = time_stamp
            results_df = pd.DataFrame.from_dict((scargc_ab.avg_perf_metric.keys(), scargc_ab.avg_perf_metric.values())).T
            # change the directory to your particular files location
            self.change_dir()
            results_scargc_ab = 'results_'+ f'{experiment}' + '.pkl' 
            results_df.to_pickle(results_scargc_ab)
            results_pkl = pd.read_pickle(results_scargc_ab)
            print("Results:\n", results_df)
        elif experiment == 'mclass':
            experiment = experiment + '_' + dataset + '_' + classifier + '_' + datasource
            mclass = mclassification.MClassification(classifier= classifier, method= method, dataset= dataset, datasource= datasource, graph= False) 
            self.results[experiment] = mclass.run()
            time_stamp = time.strftime("%Y%m%d-%H:%M:%S")
            mclass.avg_perf_metric['Experiment'] = experiment 
            mclass.avg_perf_metric['Time_Stamp'] = time_stamp
            results_df = pd.DataFrame.from_dict((mclass.avg_perf_metric.keys(), mclass.avg_perf_metric.values())).T
            # change the directory to your particular files location
            self.change_dir()
            results_mclass = 'results_'+ f'{experiment}' + '.pkl' 
            results_df.to_pickle(results_mclass)
            results_pkl = pd.read_pickle(results_mclass)
            print("Results:\n", results_df)
    def run_experiment(self, experiment, classifier, dataset, datasource, method):
        self.createExperiment(experiment=experiment, classifier=classifier, datasource=datasource, dataset=dataset, method=method)
        
    def run(self):
        processes = []
        for experiment in self.experiments:
            for classifier in self.classifiers:
                for datasource in self.datasources:
                    for dataset in self.datasets:
                        for method in self.methods:
                            process = multiprocessing.Process(target=self.run_experiment, args=(experiment, classifier, dataset, datasource, method))
                            processes.append(process)
                            process.start()
        
        # Wait for all processes to complete
        for process in processes:
            process.join()
        # self.plot_results()

if __name__ == '__main__':
    experiments = [ 'scargc']
    classifiers = ['svm', 'mlp','naive_bayes', 'lstm', 'random_forest', 'logistic_regression', 'gru', '1dcnn', 'adaboost', '1nn', 'knn', 'decision_tree']
    datasets = ['JITC']
    datasources = ['UNSW']
    methods = ['kmeans']

    # experiments = ['mclass']
    # classifiers = ['lstm']
    # datasets = ['bot_iot']
    # datasources = ['UNSW']
    # methods = ['kmeans']

    run_experiment = RunExperiment(experiments=experiments, classifiers=classifiers, datasets=datasets, datasources=datasources, methods=methods)
    run_experiment.run()


