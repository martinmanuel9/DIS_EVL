import warnings

warnings.filterwarnings("ignore")
import os

os.environ['PYTHONHASHSEED'] = '0'
import numpy as np
import tensorflow as tf
import pandas as pd
import random as rn
import joblib

from sklearn.preprocessing import MinMaxScaler
from tensorflow.compat.v1.keras import backend as K

rs = 1
np.random.seed(rs)
rn.seed(rs)
session_conf = tf.compat.v1.ConfigProto(intra_op_parallelism_threads=1,
                                        inter_op_parallelism_threads=1)

tf.compat.v1.set_random_seed(rs)
sess = tf.compat.v1.Session(graph=tf.compat.v1.get_default_graph(), config=session_conf)
K.set_session(sess)




class IncrementalLearning():
    online_model_path = 'online_models/model_online'
    online_scaler_path = 'online_models/scaler_online'

    def __init__(self, deployed_model, layer, noise_factor=0.1, loc=0.0,
                 scale=1.0, epochs=1, batch_size=1, num_sigma=5, 
                 verbose=True, shuffle=False, strict_online_learning_mode=True):
  
        self.deployed_model = deployed_model
        self.noise_factor = noise_factor
        self.loc = loc
        self.scale = scale
        self.verbose = verbose
        self.epochs = epochs
        self.batch_size = batch_size
        self.num_sigma = num_sigma
        self.shuffle = shuffle
        self.strict_online_learning_mode = strict_online_learning_mode
        self.layer = layer

    def __str__(self):
        if self.strict_online_learning_mode:
            return 'strict online learning at each step on normal behaviors\n'
        else:
            return 'batch online learning due to detecting concept drift\n'

    def incremental_build(self, X_model, X_scaler):
        """
        Parameters
        ----------
        X_model : data for updating AE

        X_scaler : data for updating scaler

        """
        
        pt = MinMaxScaler()
        pt.fit(X_scaler)
        X_scaler = pt.transform(X_scaler)
        X_model = pt.transform(X_model)
        X_model_de = X_model + self.noise_factor * np.random.normal(loc=self.loc, scale=self.scale, size=X_model.shape)
        X_scaler_de = X_scaler + self.noise_factor * np.random.normal(loc=self.loc, scale=self.scale,
                                                                      size=X_scaler.shape)

        self.deployed_model.fit(X_model_de, X_model,
                                epochs=self.epochs,
                                batch_size=self.batch_size,
                                shuffle=self.shuffle,
                                verbose=self.verbose,
                                )

        joblib.dump(pt, IncrementalLearning.online_scaler_path + '_' + self.layer + '.pkl')
        self.deployed_model.save(IncrementalLearning.online_model_path + '_' + self.layer + '.h5')

        # updating threshold using the data combining 
        # stream data in the stream storaged window and the offline training data 
        if not self.strict_online_learning_mode:
            # update threshold only when change point is detected during stream prediction and concept drift detection mode

            pred_X_scaler_de = self.deployed_model.predict(X_scaler_de)

            recontrcution_error = np.sqrt(np.sum(np.power(X_scaler - pred_X_scaler_de, 2), axis=1) / X_scaler.shape[1])

            reconstruction_train = pd.DataFrame({'reconstruction_error': recontrcution_error,
                                                })

            res_stat = reconstruction_train['reconstruction_error'].describe()
            population_threshold = res_stat[res_stat.index == 'mean'][0] + self.num_sigma * \
                                res_stat[res_stat.index == 'std'][0]
            # print(population_threshold)

            return population_threshold
