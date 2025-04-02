import warnings

warnings.filterwarnings("ignore")
import os

os.environ['PYTHONHASHSEED'] = '0'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import numpy as np
import tensorflow as tf

tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
import pandas as pd
import random as rn
import matplotlib.pyplot as plt
import time
import joblib
import pickle
import copy
import argparse

from sklearn.utils import resample
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import KFold


from itertools import compress
from sklearn.preprocessing import MinMaxScaler
from itertools import compress
from pprint import pprint

import numpy as np
np.float = float

import tensorflow as tf
from tensorflow.keras import initializers
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense, Dropout
from tensorflow.keras.callbacks import ModelCheckpoint
from tensorflow.keras import backend as K
from tensorflow.keras import regularizers

from skmultiflow.drift_detection.hddm_a import HDDM_A
from skmultiflow.drift_detection.hddm_w import HDDM_W
from skmultiflow.drift_detection.adwin import ADWIN
from skmultiflow.drift_detection import KSWIN
import pickle 


rs = 1
np.random.seed(rs)
rn.seed(rs)
session_conf = tf.compat.v1.ConfigProto(intra_op_parallelism_threads=1,
                                        inter_op_parallelism_threads=1)

tf.compat.v1.set_random_seed(rs)
sess = tf.compat.v1.Session(graph=tf.compat.v1.get_default_graph(), config=session_conf)

# K.set_session(sess)

ae_model = 'ae_offline_model'
ae_scaler = 'ae_offline_scaler'
change_detector_path = 'change_detector'
# universal_feat_path = 'files/feature_set_universal'
stream_threshold = 'JITC_stream_threshold.csv'
# kafka_data_collection_dir = 'kafka_data_collection_train'
use_cv_threshold = True


def autoencoder(X_train, X_val,
                noise_factor, epoch_train,
                activation_func, hidden_neurons, num_sigma,
                cross_validation):
    K.clear_session()
    pt = MinMaxScaler()
    # pt = StandardScaler()
    X_train = pt.fit_transform(X_train)
    X_val = pt.transform(X_val)
    X_train_de = X_train + noise_factor * np.random.normal(loc=0.000, scale=1.0, size=X_train.shape)
    X_val_de = X_val + noise_factor * np.random.normal(loc=0.000, scale=1.0, size=X_val.shape)

    my_init = initializers.glorot_uniform(seed=rs)
    input_dim = X_train_de.shape[1]
    input_layer = Input(shape=(input_dim,))

    encoder = Dense(hidden_neurons[0], activation=activation_func,
                    # activity_regularizer=regularizers.l2(1e-4)
                    kernel_initializer=my_init,
                    kernel_regularizer=regularizers.l2(1e-4),
                    # bias_regularizer=regularizers.l2(1e-4)
                    )(input_layer)
    # encoder = BatchNormalization()(encoder)
    encoder = Dropout(0.05)(encoder)
    encoder = Dense(hidden_neurons[1], activation=activation_func,
                    kernel_initializer=my_init,
                    activity_regularizer=regularizers.l2(2e-4),
                    # kernel_regularizer=regularizers.l2(1e-4)
                    )(encoder)
    # encoder = BatchNormalization()(encoder)
    encoder = Dropout(0.1)(encoder)
    decoder = Dense(hidden_neurons[2], activation=activation_func,
                    kernel_initializer=my_init,
                    # activity_regularizer=regularizers.l2(1e-4)
                    kernel_regularizer=regularizers.l2(1e-4),
                    # bias_regularizer=regularizers.l2(1e-4)
                    )(encoder)
    decoder = Dropout(0.05)(decoder)
    # decoder = BatchNormalization()(decoder)

    decoder = Dense(input_dim, activation='linear',
                    kernel_initializer=my_init,
                    )(decoder)

    autoencoder_model = Model(inputs=input_layer, outputs=decoder)

    autoencoder_model.compile(optimizer='sgd', loss='mse', metrics=['mse', 'mae'])

    checkpointer = ModelCheckpoint(
        filepath='ae_offline_model_JITC'+ '.keras',
        verbose=0,
        save_best_only=True)

    # use one-class data to train
    autoencoder_model.fit(X_train_de, X_train,
                            epochs=epoch_train,
                            batch_size=1,
                            shuffle=True,
                            verbose=1,
                            callbacks=[checkpointer])
    # autoencoder_model.summary()

    pred_val = autoencoder_model.predict(X_val_de)
    # mae
    # recontrcution_error = np.sum(np.abs(X_test - pred_test), axis=1)/input_dim

    # rmse
    recontrcution_error = np.sqrt(np.sum(np.power(X_val - pred_val, 2), axis=1) / input_dim)

    # mse
    # recontrcution_error = np.sum(np.abs(X_test - pred_test), axis=1)/input_dim

    reconstruction_val = pd.DataFrame({'reconstruction_error': recontrcution_error})

    res_stat_val = reconstruction_val['reconstruction_error'].describe(
        percentiles=[.80, .90, .95, .96, .97, .98, .99])
    # print(res_stat_val)
    population_threshold = res_stat_val[res_stat_val.index == 'mean'][0] + num_sigma * \
                        res_stat_val[res_stat_val.index == 'std'][0]
    # max_recserror = res_stat_train[res_stat_train.index == 'max'][0]

    if cross_validation:
        print('\npopulation threshold of a single round of cross validation: ', population_threshold)
        return population_threshold  # learn threshold by cross validation

    elif not cross_validation:
        joblib.dump(pt, ae_scaler + '_' + 'JITC' + '.pkl')
        autoencoder_model.save(ae_model + '_' + 'JITC' + '.h5')
        autoencoder_model.summary()
        print('\n' + 'JITC' + ' layer population threshold without cross validation: ', population_threshold)
        return population_threshold  # for comparison


def train_multiple_layer(n_folds: int, noise_factor: float, epoch: int, activation_func: str,
                        hidden_neurons: list, num_sigma: float, change_detection_model: str, remove_cv_outlier):
    
    ###--- comment the next two lines to troubleshoot (debug)-- ###
    print(os.getcwd())
    os.chdir('../')
    
    file_path = os.path.join(os.getcwd(), 'evl_streaming_src', 'datasets', 'UA_JITC_train_Bits_Clustered_Dataframe.pkl')
    with open(file_path, 'rb') as file:
        jitc_dataframe = pickle.load(file)

    print("Columns in the dataset:", jitc_dataframe.columns)

    df_normal = jitc_dataframe[jitc_dataframe['chunk_label'] == 0].copy()
    print(f"All chunks: {len(jitc_dataframe)}; Normal chunks: {len(df_normal)}")

    X = df_normal[['bit_number_scaled']].values 
    y = np.zeros(len(X))  # all normal

    skf = KFold(n_splits=n_folds, shuffle=True, random_state=rs)
    skf.get_n_splits(X, y)
    j = 1
    base_recserror_list = []
    print('Cross validation for getting autoencoder threshold on ' + 'JITC' + '...')

    for train_index, val_index in skf.split(X, y):
        print('\n\n******** ' + 'JITC' + ' layer cross validation round %d *******' % j)
        # print("train index:", train_index)
        # print("validation index:", val_index)
        ae_train, ae_val = X[train_index], X[val_index]
        print(ae_train.shape)
        print(ae_val.shape)
        base_recserror_cv = autoencoder(ae_train, ae_val,
                                        noise_factor, epoch, activation_func,
                                        hidden_neurons, num_sigma,
                                        cross_validation=True)
        base_recserror_list.append(base_recserror_cv)
        j += 1
    print('\nThreshold list: ', base_recserror_list, end='\n\n')

    def modified_z_score_outlier(cv_point, z_threshold=3.5):
        """
        remove cv threshold outlier by modified z-score  (e.g., cv thresholds [100, 0.31, 0.33, 0.35, 0.32])
        modified_z_score = 0.6745 * diff / median_absolute_deviation
        3.5 is the common modified z_threshold

        """
        cv_point = np.array(cv_point)

        if len(cv_point.shape) == 1:
            cv_point = cv_point[:, None]

        median = np.median(cv_point, axis=0)
        diff = np.sum((cv_point - median) ** 2, axis=-1)
        diff = np.sqrt(diff)
        median_absolute_deviation = np.median(diff)

        modified_z_score = 0.6745 * diff / median_absolute_deviation
        return modified_z_score < z_threshold

    if remove_cv_outlier:
        base_recserror_list = list(compress(base_recserror_list, modified_z_score_outlier(base_recserror_list)))

    print("\nRemoving outlier threshold by modified_z_score if necessary: ", base_recserror_list, end='\n\n')

    base_recserror = sum(base_recserror_list) / len(base_recserror_list)
    print('Cross validation finished...')

    print('\n********* Round of getting autoencoder model and scaler using full data *********')

    base_recserror_non_cv = autoencoder(X, X, noise_factor, epoch, activation_func,
                                        hidden_neurons, num_sigma,
                                        cross_validation=False)
    print('\n' + 'JITC' + ' layer average population threshold of cross validation: ', base_recserror, end='\n\n')

    if use_cv_threshold:
        pd.DataFrame({'Stream_threshold': [base_recserror]}).to_csv('JITC' + '_' + stream_threshold, index=False)
        print('JITC' + ' layer final threshold: ', base_recserror)

    elif not use_cv_threshold:
        pd.DataFrame({'Stream_threshold': [base_recserror_non_cv]}).to_csv('JITC' + '_' + stream_threshold, index=False)
        print('JITC' + ' layer final threshold: ', base_recserror)

    print('\nComplete training model...\n')

    if change_detection_model == "HDDM_A":
        change_detector = HDDM_A()
    elif change_detection_model == "HDDM_W":
        change_detector = HDDM_W()
    elif change_detection_model == "ADWIN":
        change_detector = ADWIN(delta=0.2)
    elif change_detection_model == "KSWIN":
        change_detector = KSWIN(alpha=0.001)

    filehandler = open(change_detector_path + '_' + 'JITC' + '.pkl', "wb")
    pickle.dump(change_detector, filehandler)
    filehandler.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", dest="model", type=str, default=None, help="Model File")
    args = parser.parse_args()
    return args


def get_ae_config():
    config = {}
    config["autoencoders"] = []

    # append SGD autoencoders for each layer
    config["autoencoders"].append(
        {"n_folds": 5, "type": "JITC", "noise_factor": 0.1, "hidden_neurons": [30, 15, 30], "num_sigma": 6,
        "epoch": 5, "activation_func": "relu"})

    # config["autoencoders"].append(
    #     {"n_folds": 5, "type": "network", "noise_factor": 0.1, "hidden_neurons": [5, 3, 5], "num_sigma": 5,
    #     "epoch": 5, "activation_func": "relu"})

    # config["autoencoders"].append(
    #     {"n_folds": 5, "type": "system", "noise_factor": 0.1, "hidden_neurons": [15, 10, 15], "num_sigma": 5,
    #     "epoch": 5, "activation_func": "relu"})

    config["change_detector"] = 'KSWIN'

    # config["select_layers"] = ["application", "network", "system"] # select the layer needed to be analyzed

    config["select_layers"] = ["JITC"]

    config["remove cross-validation outlier"] = True


    # config["select layers"] = ["application"]  # ["application", "network", "system"]

    # single round train validation splitting has risk (although low-probability) to obtain a very large threshold
    # when training data size is small (e.g., after random splitting, a feature is 0 on training data while large on validation data,
    # this results large threshold)
    # Therefore, cross-validation with removing cv outliears by modified_z_score is used to get robust threshold)

    return config


def main():
    # ae_training setting
    args = parse_args()
    ae_config = get_ae_config()
    print("\n ***** Training Parameters Setting ****** ")
    pprint(ae_config)

    if not os.path.exists('models'):
        os.mkdir('models')

    # the c# version data collector only have application layer data
    for layer in ae_config["select_layers"]:
        layer_config = list(filter(lambda x: x['type'] == layer, ae_config['autoencoders']))[0]
        print('********' + layer + '*********')
        print(layer_config)
        train_multiple_layer(
                            n_folds=layer_config["n_folds"],
                            noise_factor=layer_config["noise_factor"],
                            hidden_neurons=layer_config["hidden_neurons"],
                            num_sigma=layer_config["num_sigma"],
                            epoch=layer_config["epoch"],
                            activation_func=layer_config["activation_func"],
                            change_detection_model=ae_config["change_detector"],
                            remove_cv_outlier=ae_config["remove cross-validation outlier"])


if __name__ == '__main__':
    main()
