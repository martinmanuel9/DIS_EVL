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
from sklearn.ensemble import RandomForestClassifier
import doc 
import rich
from art import *

import numpy as np
import tensorflow as tf
import pandas as pd
from online_learning import IncrementalLearning
from feature_extraction_enhencement import feat_extract_enhence

from typing import Dict
from datetime import datetime
from tensorflow.compat.v1.keras.models import load_model

from files.feature_set import FEATURE_SET
from files.monitored_process import MONITORED_PROCESS

from termcolor import colored

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

    # stream_sample_path = 'stream_sample.csv'

    # offline_train_data_path = 'df_train_sel_feat_app.csv'

    # online_models_save_dir = 'online_models'

    # online_model_path = 'online_models/model_online_app.h5'

    # online_scaler_path = 'online_models/scaler_online_app.pkl'

    stream_sample_path = 'stream_sample'

    offline_train_data_path = 'df_train_sel_feat'

    online_models_save_dir = 'online_models'

    online_model_path = 'online_models/model_online'

    online_scaler_path = 'online_models/scaler_online'

    metric_path = 'metric'

    # one period: strict online learning phase (small numbers of samples, within 2 min in c# version) + retraining (with low
    # reconstruction sample) with concept drift detection phase (a large number of samples)

    # if strict_online_learning = True
    # performoing strict online learning from start sample point to a certain point,
    # then perform online retraining with concept drift detection.

    # if strict_online_learning = False
    # only perform online retraining with concept drift detection.

    strict_online_learning = True

    # After one period, reset current model to the initial (off-line) trained model, and then start a new period.
    # The reset procedure is important because attack pattern unluckly learned during the stric online learning
    # phase in the current period can be deleted and therefore the attack still can be detected in the next period.
    periodical_forgotten_scheme = True

    change_detector_mode = 0  # change detector_mode 0: input reconstruction error of ae; mode 1: input prediction of ae

    def __init__(self, threshold: int, layer: str, noise=0.1, mode=0, window=1000, sample_period=10000,
                 strict_online_learning_length=20, stream_sample_window=5000, ae_pred_window_size=3,
                 offline_n_sample=5000):
        self.model = None
        self.scaler = None
        self.change_detector = None
        self.threshold = threshold
        self.layer = layer
        self.noise = noise
        self.mode = mode
        self.data = []
        self.window = window
        self.sample_count_in_period = 0
        self.sample_index = 0
        self.sample_period = sample_period
        self.df_win_out_two = pd.DataFrame()
        self.stream_sample_load = pd.DataFrame()
        self.offline_train_data = None
        self.strict_online_learning_length = strict_online_learning_length
        self.stream_sample_window = stream_sample_window
        self.metric_path = MachineLearning.metric_path + '_' + self.layer + '.csv'
        self.init_threshold = threshold
        self.ae_pred_window = []
        self.ae_pred_window_size = ae_pred_window_size
        self.ae_pred_window_vote = None
        self.offline_n_sample = offline_n_sample
        # the first ten windowing vote predictions (around total ten seconds) in the strict
        # online learning phase won't be used to trigger alarm
        self.mute_alarm_strict_online_learning_size = 10


        self.attack_cls_model = None
        self.attack_cls_scaler = None
        self.attack_val_map = {
            0: 'abuse_msiexec',
            1: 'abuse_regsvr',
            2: 'abuse_rundll32',
            3: 'application_window_discovery',
            4: 'automated_exfiltration',
            5: 'exfiltration_http',
            6: 'invoke_app_path_bypass',
            7: 'invoke_html_app',
            8: 'malicious_copy',
            9: 'process_injection',
            10: 'ransomware'
        }

        MachineLearning.stream_sample_path = MachineLearning.stream_sample_path + '_' + self.layer + '.csv'
        MachineLearning.offline_train_data_path = MachineLearning.offline_train_data_path + '_' + self.layer + '.csv'
        MachineLearning.online_model_path = MachineLearning.online_model_path + '_' + self.layer + '.h5'
        MachineLearning.online_scaler_path = MachineLearning.online_scaler_path + '_' + self.layer + '.pkl'

        '''
        random sampling the offline training data used for combining the stream data to
        update model (ae model (h5 file), normalization scaler (pkl fiel), and threshold.
        Since the offline data collected by c# version might be very large.
        The random sampling procedure can reduce processing time during strict online learning for each sample,
        and model updating due to drift detection.
        '''
        self.offline_train_data = pd.read_csv(MachineLearning.offline_train_data_path)
        if self.offline_train_data.shape[0] > self.offline_n_sample:
            self.offline_train_data = self.offline_train_data.sample(n=self.offline_n_sample, replace=False)
        print(self.offline_train_data.shape)

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

    def load_models_attack_cls(self):
        # self.attack_cls_model = load_model('attack_cls/mlp0.h5')
        self.attack_cls_model = joblib.load('attack_files/rf_model.pkl')
        self.attack_cls_scaler = joblib.load('attack_files/scaler.pkl')
        # self.attack_cls_model = joblib.load('attack_files/random_forest.pkl')
        # self.attack_cls_scaler = joblib.load('attack_files/rf_sc.pkl')


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

        if os.path.exists(MachineLearning.stream_sample_path):
            os.remove(MachineLearning.stream_sample_path)

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

    def printProgressBar(self, i, max, postText):

        # progress bar for strict online learning phase
        n_bar = 50  # size of progress bar
        j = i / max
        # sys.stdout.write('\r')
        # sys.stdout.write(f"[{'=' * int(n_bar * j):{n_bar}s}] {int(100 * j)}%  {postText}")
        # sys.stdout.flush()

        print(f"[{'=' * int(n_bar * j):{n_bar}s}] {int(100 * j)}%  {postText}", end='')

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

    def merge_process_sequence(self, df):

        df = df[df['Description'].isin(MONITORED_PROCESS)]

        # print(df_win)

        df = df.fillna(0)

        if self.mode == 0:
            df = df.groupby('Description').sum()
        else:
            df = df.groupby('Description').mean()

        app_cols_list = list(df.keys())
        applications = list(df.index)

        df_rename = pd.DataFrame(df.values.reshape(1, -1))
        count = 0
        for combined in itertools.product(applications, app_cols_list):
            df_rename = df_rename.rename(
                columns={count: "{}_{}".format(combined[0], combined[1])})
            count += 1

        df_col = pd.DataFrame(columns=FEATURE_SET)
        col_diff_app = df_col.columns.difference(df_rename.columns)
        df_out = pd.concat([df_rename, df_col[col_diff_app]], axis=1).fillna(0)

        df_out = df_out[FEATURE_SET]

        return df_out


    # def attack_classification(self, df):

    #     x_cls = df.values.reshape(1, -1)
    #     x_cls = self.attack_cls_scaler.transform(x_cls)
    #     y_cls = self.attack_cls_model.predict(x_cls)
    #     # y_cls = np.argmax(y_cls, axis=1)
    #     print(y_cls)
    #     # print(self.attack_val_map[y_cls[0]])

    #     # print("\n\nattack classification -->", self.attack_val_map[y_cls[0]])


    #     s = f"""
    #     {'-'*40}
    #     # Attack Classification
    #     # Host_Behaviors: 'Abnormal'
    #     # Attack: {colored(y_cls, 'green')}
    #     {'-'*40}
    #     """

    #     print(s)
    #     doc.ontology_display(y_cls)


    def attack_classification(self, df):

        x_cls = df.values.reshape(1, -1)
        x_cls = self.attack_cls_scaler.transform(x_cls)
        y_cls = self.attack_cls_model.predict(x_cls)
        # y_cls = np.argmax(y_cls, axis=1)
        tprint(''' attack classification phase''')
        tprint(str(y_cls))

        # print(y_cls)
        # print(self.attack_val_map[y_cls[0]])

        # print("\n\nattack classification -->", self.attack_val_map[y_cls[0]])

        s = f"""
        {'-'*40}
        # Host_Behaviors: 'Abnormal'
        # Attack: {colored(y_cls, 'green')}
        {'-'*40}
        """

        print(s)
        doc.ontology_display(y_cls)


    def process(self) -> Dict:

        df_win = pd.DataFrame(data=self.data)

        suspecious_name_cnt = feat_extract_enhence(df_win)

        df_win = df_win.drop(columns=['ExecutablePath'], axis=1)

        df_win_out = self.merge_process_sequence(df_win)

        df_win_out['suspecious_name_cnt'] = suspecious_name_cnt

        df_win_out = df_win_out.reindex(sorted(df_win_out.columns), axis=1)


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

            diff_cols = [col
                         for col in df_win_out.columns
                         if 'count' in col or 'system' in col
                         ]

            df_win_out[diff_cols] = self.df_win_out_two[diff_cols].diff().iloc[-1:].reset_index(drop=True)

            self.df_win_out_two = self.df_win_out_two.iloc[-1:]

            # all the stream sample (normal and abnormal), just used for post analysis
            df_win_out.to_csv(self.layer + '_all_stream_sample.csv', index=False, mode='a',
                              header=not os.path.exists(self.layer + '_all_stream_sample.csv'))

            ae_pred, res_error = self.predict(df_win_out)


            ### Majority Windowing Prediction ###
            # prediction window with majority vote: [0, 1, 0] = 0; [0, 1, 1] = 1
            self.ae_pred_window.append(ae_pred)
            if len(self.ae_pred_window) < self.ae_pred_window_size:
                pass
            else:
                self.ae_pred_window_vote = max(set(self.ae_pred_window), key=self.ae_pred_window.count)
                self.ae_pred_window.pop(0)

            print("self.ae_pred_window_vote:   ", self.ae_pred_window_vote)


            #### attack cls #######

            if self.sample_count_in_period > self.mute_alarm_strict_online_learning_size:
                if self.ae_pred_window_vote:
                    print("self.ae_pred_window_vote 2:   ", self.ae_pred_window_vote)

                    self.attack_classification(df_win_out)


            ### change detection analysis ###

            one_row_reserror = res_error[0]

            change_detector_in = [one_row_reserror, ae_pred]

            self.change_detector.add_element(change_detector_in[MachineLearning.change_detector_mode])

            if self.change_detector.detected_change():
                change_point = 1
            else:
                change_point = 0

            ### save stream sample ###
            if MachineLearning.strict_online_learning and self.sample_count_in_period <= self.strict_online_learning_length:
                # when each period restart (i.e., reset model to initial offline trained model (h5, scaler.pkl, threshold, and
                # change detector)),
                # the first ten windowing predictions (each prediction is around 1 second) in the strict online learning
                # phase won't be considered to trigger abnormal prediction.
                if self.sample_count_in_period < self.mute_alarm_strict_online_learning_size:
                    self.ae_pred_window_vote = 0

                # save stream samples at each step in strict online learning phase ( around 100 seconds in c# version)
                if not os.path.exists(MachineLearning.stream_sample_path):
                    df_win_out.to_csv(MachineLearning.stream_sample_path, index=False, mode='w')
                else:
                    df_win_out.to_csv(MachineLearning.stream_sample_path, index=False, mode='a',
                                      header=not os.path.exists(MachineLearning.stream_sample_path))

            elif res_error[0] <= self.threshold:

                # save stream samples with low reconstruction errors in drift detection phase
                if not os.path.exists(MachineLearning.stream_sample_path):
                    df_win_out.to_csv(MachineLearning.stream_sample_path, index=False, mode='w')
                else:
                    df_win_out.to_csv(MachineLearning.stream_sample_path, index=False, mode='a',
                                      header=not os.path.exists(MachineLearning.stream_sample_path))

            ### updating models ###
            if os.path.exists(MachineLearning.stream_sample_path):

                if MachineLearning.strict_online_learning and self.sample_count_in_period <= self.strict_online_learning_length:

                    percent = str(self.sample_count_in_period) + '/' + str(self.strict_online_learning_length)
                    self.printProgressBar(self.sample_count_in_period, self.strict_online_learning_length, percent)

                    print('  << application >> incremental learning on Normal data')

                    self.stream_sample_load = pd.read_csv(MachineLearning.stream_sample_path)

                    online_model = IncrementalLearning(self.model, layer=self.layer, strict_online_learning_mode=True)
                    # print(online_model)

                    # strict online learning AE at the sample of each step
                    # incremental learning starts with initial model and the required steps are small,
                    # for the c# version data collector, the strict online learning will be finished within 2 min.
                    # the "ae model (h5 file)" and "normalization scaler (pkl file)" are updated in this phase
                    online_model.incremental_build(self.stream_sample_load[-1:],
                                                   pd.concat([self.stream_sample_load,
                                                              self.offline_train_data])
                                                   )

                    self.model = load_model(MachineLearning.online_model_path)
                    self.scaler = joblib.load(MachineLearning.online_scaler_path)

                elif change_point:

                    # maintain the stream sample storaging window size
                    self.stream_sample_load = pd.read_csv(MachineLearning.stream_sample_path)[
                                              -self.stream_sample_window:]
                    self.stream_sample_load.to_csv(MachineLearning.stream_sample_path, index=False)

                    print(
                        '\n\n *** ' + self.layer + ' layer change point has been detected. Peforming incremental retraining \n\n')
                    # self.stream_sample_load = pd.read_csv(MachineLearning.stream_sample_path)

                    online_model = IncrementalLearning(self.model, layer=self.layer, strict_online_learning_mode=False)
                    # print(online_model)

                    # incremental retraining on low reconstruction error samples if drift is detected
                    # the "ae model (h5 file)", "normalization scaler (pkl file)" and "threshold" are updated in drift analysis phase.

                    self.threshold = online_model.incremental_build(self.stream_sample_load,
                                                                    pd.concat([self.stream_sample_load,
                                                                               self.offline_train_data])
                                                                    )

                    self.model = load_model(MachineLearning.online_model_path)
                    self.scaler = joblib.load(MachineLearning.online_scaler_path)

                else:
                    print('\n <<< application >>> stream prediction and concept drift detection mode\n')

            sample_time = datetime.fromtimestamp(self.data[-1]["TimeStamp"], pytz.timezone("US/Arizona"))
            # print('time of sample: ', sample_time)


            ### recording metrics ###
            ae_one_row = {
                'layer': self.layer,
                'sample_index ': self.sample_index,
                'self.sample_count_in_period': self.sample_count_in_period,
                'prediction': ae_pred,
                'prediction_window_vote': self.ae_pred_window_vote,
                'reconstruction_error': one_row_reserror,
                'change_point': change_point,
                'threshold': self.threshold,
                'p_values': self.change_detector.p_value,
                'time': sample_time.strftime("%H:%M:%S.%f - %b %d %Y")
            }

            pd.DataFrame([ae_one_row]).to_csv(self.metric_path, index=False, mode='a',
                                              header=not os.path.exists(self.metric_path))

            self.sample_index += 1
            self.sample_count_in_period += 1


            ### resetting to initial offline trained model (ae.h5, scaler.pkl, threshold, and change detector) ###
            '''
            The resetting procedure is important. It can avoid model permanently learn the attacks.
            After a period, reset to initial models, then start a new period.
            Reset to initial models means reset the "threshold", "h5 model", "normalization scaler", and "change detector".
            '''
            if MachineLearning.periodical_forgotten_scheme and self.sample_count_in_period % self.sample_period == 0:
                self.sample_count_in_period = 0
                self.load_models(platform="Windows")  # reset to initial "h5 model" and initial "normalization scaler".
                self.threshold = self.init_threshold  # reset to inttial threshold
                self.change_detector.reset()  # reset the change dector.

            # tprint(''' Anomaly detection phase''')
            print(ae_one_row)

            # print('\n\n')
            return ae_one_row
