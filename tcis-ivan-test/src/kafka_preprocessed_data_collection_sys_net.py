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

from typing import Dict
from datetime import datetime
from tensorflow.compat.v1.keras.models import load_model

from files.feature_set import FEATURE_SET
from files.monitored_process import MONITORED_PROCESS

warnings.filterwarnings("ignore")


class PreprocessingCollection():
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

    save_dir = 'kafka_data_collection_train'

    reset_trianing_data = True

    def __init__(self, layer: str, threshold: int, mode=0):
        self.model = None
        self.scaler = None
        self.change_detector = None
        self.layer = layer
        self.threshold = threshold
        self.mode = mode
        # self.data = []
        self.sample_index = 0

        self.df_win_out_two = pd.DataFrame()

        self.save_dir = PreprocessingCollection.save_dir + '_' + layer
        self.train_sample_path = self.save_dir + '/' + 'normal_train.csv'

        if not os.path.exists(self.save_dir):
            os.mkdir(self.save_dir)

        if os.path.exists(self.save_dir) and PreprocessingCollection.reset_trianing_data:
            for f in os.listdir(self.save_dir):
                os.remove(os.path.join(self.save_dir, f))

    def process(self, data) -> Dict:

        # print(data)
        df_win_out = pd.DataFrame([data])

        df_win_out = df_win_out.reindex(sorted(df_win_out.columns), axis=1)

        sample_time = datetime.fromtimestamp(df_win_out["time"], pytz.timezone("US/Arizona"))

        df_win_out = df_win_out.drop(labels=['time'], axis=1)

        if self.sample_index == 0:

            self.df_win_out_two = self.df_win_out_two.append(df_win_out, ignore_index=False)
            self.sample_index += 1

        else:
            print('\n' + self.layer + ' layer training sample count ', self.sample_index)
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

            # print(df_win_out.to_dict())

            self.sample_index += 1

            if not os.path.exists(self.train_sample_path):
                df_win_out.to_csv(self.train_sample_path, index=False, mode='w')
            else:
                df_win_out.to_csv(self.train_sample_path, index=False, mode='a',
                                  header=not os.path.exists(self.train_sample_path))

            print('time of sample: ', sample_time)
            print('<< ' + self.layer + '>> layer data colleciton and preprocessing mode\n\n')
        return None
