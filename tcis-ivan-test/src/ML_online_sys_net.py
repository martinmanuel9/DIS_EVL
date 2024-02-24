import warnings
import joblib
import pickle
import pathlib
import itertools
import logging
import os
import pytz
import sys
import time

import numpy as np
import tensorflow as tf
import pandas as pd
from online_learning import IncrementalLearning

from typing import Dict
from datetime import datetime
from tensorflow.compat.v1.keras.models import load_model

from files.feature_set import FEATURE_SET
from files.monitored_process import MONITORED_PROCESS

warnings.filterwarnings("ignore")


class MachineLearning():
    tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
    logging.basicConfig(
        format="%(name)s:%(levelname)s:%(asctime)s: %(message)s",
        level=logging.WARNING,
        datefmt="%d-%b-%y %H:%M:%S",
    )
    handler = logging.StreamHandler()
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)

    # path setup
    globs = ["models/*.h5", "models/*.pkl"]

    # stream_sample_path = 'stream_sample_sys.csv'

    # offline_train_data_path = 'df_train_sel_feat_sys.csv'

    # online_models_save_dir = 'online_models'

    # online_model_path = 'online_models/model_online_sys.h5'

    # online_scaler_path = 'online_models/scaler_online_sys.pkl'



    stream_sample_path = 'stream_sample'

    offline_train_data_path = 'df_train_sel_feat'

    online_models_save_dir = 'online_models'

    online_model_path = 'online_models/model_online'

    online_scaler_path = 'online_models/scaler_online'

    metric_path = 'metric'


    # one period: strict online learning phase (small numbers of samples) within 10 min + retraining (with low reconstruction sample) with concept drift detection phase (a large number of samples)

    # if strict_online_learning = True
    # performoing strict online learning from start sample point to a certain point,
    # then perform online retraining with concept drift detection  

    # if strict_online_learning = False
    # only perform online retraining with concept drift detection

    strict_online_learning = True

    # after one period, reset current model to the initial (off-line) model, and then start a new period
    periodical_forgotten_scheme = True

    change_detector_mode = 0

    # whether keep cv threshold in the strict online learning phase
    keep_cv_threshold = True

    def __init__(self, threshold: int, layer: str, noise=0.1, mode=0, sample_period=2000,
                 strict_online_learning_length=45, stream_sample_window=1955):
        self.model = None
        self.scaler = None
        self.change_detector = None
        self.threshold = threshold
        self.layer = layer
        self.noise = noise
        self.mode = mode
        self.data = []
        self.sample_count_in_period = 0
        self.sample_index = 0
        self.sample_period = sample_period   
        self.df_win_out_two = pd.DataFrame()
        self.stream_sample_load = pd.DataFrame()
        self.offline_train_data = None
        self.strict_online_learning_length = strict_online_learning_length
        self.stream_sample_window = stream_sample_window
        self.init_threshold= self.threshold


        self.stream_sample_path = MachineLearning.stream_sample_path + '_' + self.layer + '.csv'
        self.offline_train_data_path = MachineLearning.offline_train_data_path + '_' + self.layer + '.csv'
        self.online_model_path = MachineLearning.online_model_path + '_' + self.layer + '.h5'
        self.online_scaler_path = MachineLearning.online_scaler_path + '_' + self.layer + '.pkl'
        self.metric_path = MachineLearning.metric_path + '_' + self.layer + '.csv'

        self.offline_train_data = pd.read_csv(self.offline_train_data_path)

        if not os.path.exists(MachineLearning.online_models_save_dir):
            os.mkdir(MachineLearning.online_models_save_dir)

        # reset metric file only when real-time analysis starts
        if os.path.exists(self.metric_path):
            os.remove(self.metric_path)

    def load_data(self, data: Dict) -> bool:
        ready = False
        if len(self.data) <= self.window:
            self.data.append(data)
        else:
            self.data.pop(0)
            self.data.append(data)
            ready = True
        return ready


    def printProgressBar(self, i,max,postText):

        # progress bar for strict online learning phase
        n_bar = 50 # size of progress bar
        j= i/max
        # sys.stdout.write('\r')
        # sys.stdout.write(f"[{'=' * int(n_bar * j):{n_bar}s}] {int(100 * j)}%  {postText}")
        # sys.stdout.flush()

        print(f"[{'=' * int(n_bar * j):{n_bar}s}] {int(100 * j)}%  {postText}", end='')

    def load_models(self, platform: str) -> None:
        """
        Description:
            loads the models into the class
            based on OS platform
        Args:
            platform: string (name of the OS model to use)
        Returns:
            None
        """

        if os.path.exists(self.stream_sample_path):
            os.remove(self.stream_sample_path)

        path = pathlib.Path().cwd()
        paths = []
        for glob in self.globs:
            paths.extend(sorted(path.glob(glob)))

        if not paths:
            self.logger.critical("no data file exists in the project")
            raise KeyboardInterrupt

        ok = False
        for path_ in paths:
            if platform in path_.name:
                if ("ae_offline_model_Windows_" + self.layer) in path_.name:
                    self.model = load_model(path_)
                    ok = True
                elif ("ae_offline_scaler_Windows_" + self.layer) in path_.name:
                    self.scaler = joblib.load(path_)
                    ok = True
                elif ("change_detector_Windows_" + self.layer) in path_.name:
                    with open(path_, "rb") as detector:
                        self.change_detector = pickle.load(detector)
                    ok = True
            if ok:
                self.logger.debug(f"loaded model {path_.name}")

    def predict(self, x_test):
        # x_test = x_test.values.reshape(1, -1)
        x_test = x_test.values
        x_test_sc = self.scaler.transform(x_test)
        x_test_sc_de = x_test_sc + self.noise * np.random.normal(loc=0.000, scale=1, size=x_test_sc.shape)

        # reconstruction error
        pred = self.model.predict(x_test_sc_de)

        # mae
        # recs_test = np.sum(np.abs(x_test_sc - pred), axis=1) / x_test_sc.shape[1]

        # rmse
        recs_test = np.sqrt(np.sum(np.power(x_test_sc - pred, 2), axis=1) / x_test_sc.shape[1])

        if recs_test < self.threshold:
            prediction = 0
        else:
            prediction = 1
        return prediction, recs_test

    def process(self, data) -> Dict:

        df_win_out = pd.DataFrame([data])

        df_win_out = df_win_out.reindex(sorted(df_win_out.columns), axis=1)


        sample_time = datetime.fromtimestamp(df_win_out["time"], pytz.timezone("US/Arizona"))
        # print('time of sample: ', sample_time)        
        
        df_win_out = df_win_out.drop(labels=['time'], axis=1)

        if self.sample_count_in_period == 0:

            self.df_win_out_two = self.df_win_out_two.append(df_win_out, ignore_index=False)
            self.sample_count_in_period += 1
            self.sample_index += 1
            print()

        else:
            # print('self.sample_count_in_period', self.sample_count_in_period)
            # print('self.sample_index', self.sample_index)
            self.df_win_out_two = self.df_win_out_two.append(df_win_out, ignore_index=False)


            self.df_win_out_two = self.df_win_out_two.reset_index(drop=True)

            if self.layer == 'system':

                diff_cols = [col
                            for col in df_win_out.columns
                            if 'io' in col or 'stats' in col or 'cpu_times' in col 
                            or 'memory' in col or 'used' in col or 'usage' in col
                            or 'cpu_frequency' in col
                            ]

            elif self.layer == 'network':

                diff_cols = [col
                            for col in df_win_out.columns
                            if 'network_io' in col 
                            ]


            df_win_out[diff_cols] = self.df_win_out_two[diff_cols].diff().iloc[-1:].reset_index(drop=True)

            self.df_win_out_two = self.df_win_out_two.iloc[-1:]


            # all the stream sample (normal and abnormal), just used for post analysis
            df_win_out.to_csv(self.layer+'_all_stream_sample.csv', index=False, mode='a',
                                              header=not os.path.exists(self.layer+'_all_stream_sample.csv'))


            ae_pred, res_error = self.predict(df_win_out)

            one_row_reserror = res_error[0]

            change_detector_in = [one_row_reserror, ae_pred]

            self.change_detector.add_element(change_detector_in[MachineLearning.change_detector_mode])

            if self.change_detector.detected_change():
                change_point = 1
            else:
                change_point = 0

            # save stream sample 

            if MachineLearning.strict_online_learning and self.sample_count_in_period <= self.strict_online_learning_length:

                # save samples at each step

                if not os.path.exists(self.stream_sample_path):
                    df_win_out.to_csv(self.stream_sample_path, index=False, mode='w')
                else:
                    df_win_out.to_csv(self.stream_sample_path, index=False, mode='a',
                                      header=not os.path.exists(self.stream_sample_path))

            elif res_error[0] <= self.threshold:

                # save samples with low reconstruction errors

                if not os.path.exists(self.stream_sample_path):
                    df_win_out.to_csv(self.stream_sample_path, index=False, mode='w')
                else:
                    df_win_out.to_csv(self.stream_sample_path, index=False, mode='a',
                                      header=not os.path.exists(self.stream_sample_path))

            # updating models

            if os.path.exists(self.stream_sample_path):

                self.stream_sample_load = pd.read_csv(self.stream_sample_path)[-self.stream_sample_window:]
                self.stream_sample_load.to_csv(self.stream_sample_path, index=False)

                if MachineLearning.strict_online_learning and self.sample_count_in_period <= self.strict_online_learning_length:

                    percent = str(self.sample_count_in_period) + '/' + str(self.strict_online_learning_length)
                    self.printProgressBar(self.sample_count_in_period, self.strict_online_learning_length, percent)


                    print('  << ' + self.layer + ' >> layer incremental_learning on Normal data') 


                    self.stream_sample_load = pd.read_csv(self.stream_sample_path)

                    online_model = IncrementalLearning(self.model, layer=self.layer, strict_online_learning_mode=True)
                    # print(online_model)

                    # strict online learning AE at the sample of each step
                    if MachineLearning.keep_cv_threshold:

                        # incremental learning starts with initial model and the required steps are small,
                        # therefore keep ininital cv threshold is an option

                        online_model.incremental_build(self.stream_sample_load[-1:],
                                                pd.concat([self.stream_sample_load,
                                                        self.offline_train_data])
                                                )


                    if not MachineLearning.keep_cv_threshold:

                        self.threshold = online_model.incremental_build(self.stream_sample_load[-1:],
                                                                        pd.concat([self.stream_sample_load,
                                                                                self.offline_train_data])
                                                                        )



                    self.model = load_model(self.online_model_path)
                    self.scaler = joblib.load(self.online_scaler_path)


                elif change_point:
                    print('\n\n *** ' + self.layer + ' layer change point has been detected. Peforming incremental retraining \n\n')
                    self.stream_sample_load = pd.read_csv(self.stream_sample_path)

                    online_model = IncrementalLearning(self.model, layer=self.layer, strict_online_learning_mode=False)
                    # print(online_model)

                    # batch online incremental retraining on low reconstruction error samples if drift is detected


                    self.threshold = online_model.incremental_build(self.stream_sample_load,
                                                                        pd.concat([self.stream_sample_load,
                                                                                    self.offline_train_data])
                                                                    )



                    self.model = load_model(self.online_model_path)
                    self.scaler = joblib.load(self.online_scaler_path)

                else:
                    print('\n <<< ' +  self.layer + ' >>> stream prediction and concept drift detection mode\n')

            ae_one_row = {
                'layer': self.layer,
                'sample_index ': self.sample_index ,
                # 'self.sample_count_in_period': self.sample_count_in_period,
                'prediction': ae_pred,
                'reconstruction_error': one_row_reserror,
                'change_point': change_point,
                'time': sample_time.strftime("%H:%M:%S.%f - %b %d %Y")
            }

            pd.DataFrame([ae_one_row]).to_csv(self.metric_path, index=False, mode='a',
                                              header=not os.path.exists(self.metric_path))


            self.sample_index += 1
            self.sample_count_in_period += 1

            if MachineLearning.periodical_forgotten_scheme and self.sample_count_in_period % self.sample_period == 0:
                # after a period, reset to initial models, then start a new period
                # reset the threshold, h5 model, and scaler
                self.sample_count_in_period = 0
                self.load_models(platform="Windows")
                self.threshold = self.init_threshold


            print(ae_one_row)


            print('\n\n')
            return ae_one_row
