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
from feature_extraction_enhencement import feat_extract_enhence

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

    save_dir = 'kafka_data_collection_train_application'

    train_sample_path = str(save_dir) + '/' + 'normal_train.csv'

    reset_trianing_data = True


    def __init__(self, threshold: int, mode=0, window=300):
        self.model = None
        self.scaler = None
        self.change_detector = None
        self.threshold = threshold
        self.mode = mode
        self.data = []
        self.window = window
        self.sample_count = 0
        self.df_win_out_two = pd.DataFrame()  # for calculating diff() of some features

        if not os.path.exists(PreprocessingCollection.save_dir):
            os.mkdir(PreprocessingCollection.save_dir)

        if os.path.exists(PreprocessingCollection.save_dir) and PreprocessingCollection.reset_trianing_data:
            for f in os.listdir(PreprocessingCollection.save_dir):
                os.remove(os.path.join(PreprocessingCollection.save_dir, f))

    def load_data(self, data: Dict) -> bool:
        ready = False
        if len(self.data) <= self.window:
            self.data.append(data)
        else:
            self.data.pop(0)
            self.data.append(data)
            ready = True
        return ready


    def merge_process_sequence(self, df):



        df = df[df['Description'].isin(MONITORED_PROCESS)]


        # print(df)

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


    def process(self) -> Dict:

        df_win = pd.DataFrame(data=self.data)

        suspecious_name_cnt = feat_extract_enhence(df_win)

        df_win = df_win.drop(columns=['ExecutablePath'], axis=1)

        df_win_out = self.merge_process_sequence(df_win)

        df_win_out['suspecious_name_cnt'] = suspecious_name_cnt

        df_win_out = df_win_out.reindex(sorted(df_win_out.columns), axis=1)

        #  the process of using two sample for calculating increment for some feature

        if self.sample_count == 0:

            self.df_win_out_two = self.df_win_out_two.append(df_win_out, ignore_index=False)
            self.sample_count += 1

        else:
            print('\napplication training sample count', self.sample_count)
            self.df_win_out_two = self.df_win_out_two.append(df_win_out, ignore_index=False)

            self.df_win_out_two = self.df_win_out_two.reset_index(drop=True)

            diff_cols = [col
                         for col in df_win_out.columns
                         if 'count' in col or 'system' in col
                         ]

            df_win_out[diff_cols] = self.df_win_out_two[diff_cols].diff().iloc[-1:].reset_index(drop=True)


            self.df_win_out_two = self.df_win_out_two.iloc[-1:]

            sample_time = datetime.fromtimestamp(self.data[-1]["TimeStamp"], pytz.timezone("US/Arizona"))

            df_win_out['date_time'] = sample_time


            if not os.path.exists(PreprocessingCollection.train_sample_path):
                df_win_out.to_csv(PreprocessingCollection.train_sample_path, index=False, mode='w')
            else:
                df_win_out.to_csv(PreprocessingCollection.train_sample_path, index=False, mode='a', header=not os.path.exists(PreprocessingCollection.train_sample_path))


            # tracking the time
            print('time of sample: ', sample_time)
            self.sample_count += 1


            print('<< application >> layer data colleciton and preprocessing mode\n\n')
            return None
