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
from owlready2 import *
from typing import Dict
from datetime import datetime
from tensorflow.compat.v1.keras.models import load_model
from feature_extraction_enhencement import feat_extract_enhence
from files.feature_set import FEATURE_SET
from files.monitored_process import MONITORED_PROCESS

import pickle
from sklearn.ensemble import VotingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn import svm
import xgboost as xgb
import lightgbm as lgb

warnings.filterwarnings("ignore")

def create_ontology():
    capec = pd.read_csv('/home/avirtekdev/sicong_bau/host_bau/ivan/preprocessing/src/files/3000.csv', error_bad_lines=False)
    capec = capec.fillna(' ')

    new_names = ['ID', 'Name', 'Abstraction', 'Status', 'Description',
           'Alternate Terms', 'Likelihood Of Attack', 'Typical Severity',
           'Related Attack Patterns', 'Execution Flow', 'Prerequisites',
           'Skills Required', 'Resources Required', 'Indicators', 'Consequences',
           'Mitigations', 'Example Instances', 'Related Weaknesses',
           'Taxonomy Mappings', 'Notes']
    capec = capec.rename({'Notes': 'delete me', 'Taxonomy Mappings': 'Notes', 'Related Weaknesses':'Taxonomy Mappings' , 'Example Instances':'Related Weaknesses' , 'Mitigations':'Example Instances' , 'Consequences':'Mitigations' , 'Indicators':'Consequences' , 'Resources Required':'Indicators' , 'Skills Required':'Resources Required' , 'Prerequisites':'Skills Required' , 'Execution Flow':'Prerequisites' , 'Related Attack Patterns':'Execution Flow' , 'Typical Severity':'Related Attack Patterns' , 'Likelihood Of Attack':'Typical Severity' , 'Alternate Terms':'Likelihood Of Attack' , 'Description':'Alternate Terms' , 'Status':'Description', 'Abstraction':'Status', 'Name':'Abstraction', 'ID':'Name', 0:'id'}, axis=1)
    capec = capec.drop(columns=['delete me'])
    capec.reset_index(level=0, inplace=True)

    onto = get_ontology('/home/avirtekdev/sicong_bau/host_bau/ivan/preprocessing/src/files/capec_ontology.owl#')

    with onto:
        #capec_list = [212,185,216,116,549]
        #attack_list = [abuse, invoke, exfiltration,malicious_copy,ransomware]

        #class Abuse(Thing): pass
        #class Invoke(Thing): pass
        #class Exfiltration(Thing): pass
        #class Malicious_copy(Thing): pass
        #class Ransomware(Thing): pass

        class Abuse(Thing):
            #@classmethod
            def capec_data(self):
                num = 212
                print('CAPEC id:', capec[capec['index']==num]['index'].iloc[0])
                print('Name: ', capec[capec['index']==num]['Name'].iloc[0])
                print('Vulnerabilties: ', capec[capec['index']==num]['Related Weaknesses'].iloc[0])
                print('Countermeasures: ', capec[capec['index']==num]['Mitigations'].iloc[0])
                print('Prerequisires: ', capec[capec['index']==num]['Prerequisites'].iloc[0])
                print('Skills needed: ', capec[capec['index']==num]['Skills Required'].iloc[0])
                print('Resources needed: ', capec[capec['index']==num]['Resources Required'].iloc[0])
                print('Attack method: ', capec[capec['index']==num]['Execution Flow'].iloc[0])
                print('Consequences: ', capec[capec['index']==num]['Consequences'].iloc[0])
                print('Severity: ', capec[capec['index']==num]['Typical Severity'].iloc[0])
                print('Attack likelihood: ', capec[capec['index']==num]['Likelihood Of Attack'].iloc[0])
        class Invoke(Thing):
            #@classmethod
            def capec_data(self):
                num = 185
                print('CAPEC id:', capec[capec['index']==num]['index'].iloc[0])
                print('Name: ', capec[capec['index']==num]['Name'].iloc[0])
                print('Vulnerabilties: ', capec[capec['index']==num]['Related Weaknesses'].iloc[0])
                print('Countermeasures: ', capec[capec['index']==num]['Mitigations'].iloc[0])
                print('Prerequisires: ', capec[capec['index']==num]['Prerequisites'].iloc[0])
                print('Skills needed: ', capec[capec['index']==num]['Skills Required'].iloc[0])
                print('Resources needed: ', capec[capec['index']==num]['Resources Required'].iloc[0])
                print('Attack method: ', capec[capec['index']==num]['Execution Flow'].iloc[0])
                print('Consequences: ', capec[capec['index']==num]['Consequences'].iloc[0])
                print('Severity: ', capec[capec['index']==num]['Typical Severity'].iloc[0])
                print('Attack likelihood: ', capec[capec['index']==num]['Likelihood Of Attack'].iloc[0])
        class Exfiltration(Thing):
            #@classmethod
            def capec_data(self):
                num = 216
                print('CAPEC id:', capec[capec['index']==num]['index'].iloc[0])
                print('Name: ', capec[capec['index']==num]['Name'].iloc[0])
                print('Vulnerabilties: ', capec[capec['index']==num]['Related Weaknesses'].iloc[0])
                print('Countermeasures: ', capec[capec['index']==num]['Mitigations'].iloc[0])
                print('Prerequisires: ', capec[capec['index']==num]['Prerequisites'].iloc[0])
                print('Skills needed: ', capec[capec['index']==num]['Skills Required'].iloc[0])
                print('Resources needed: ', capec[capec['index']==num]['Resources Required'].iloc[0])
                print('Attack method: ', capec[capec['index']==num]['Execution Flow'].iloc[0])
                print('Consequences: ', capec[capec['index']==num]['Consequences'].iloc[0])
                print('Severity: ', capec[capec['index']==num]['Typical Severity'].iloc[0])
                print('Attack likelihood: ', capec[capec['index']==num]['Likelihood Of Attack'].iloc[0])
        class Malicious_copy(Thing):
            #@classmethod
            def capec_data(self):
                num = 116
                print('CAPEC id:', capec[capec['index']==num]['index'].iloc[0])
                print('Name: ', capec[capec['index']==num]['Name'].iloc[0])
                print('Vulnerabilties: ', capec[capec['index']==num]['Related Weaknesses'].iloc[0])
                print('Countermeasures: ', capec[capec['index']==num]['Mitigations'].iloc[0])
                print('Prerequisires: ', capec[capec['index']==num]['Prerequisites'].iloc[0])
                print('Skills needed: ', capec[capec['index']==num]['Skills Required'].iloc[0])
                print('Resources needed: ', capec[capec['index']==num]['Resources Required'].iloc[0])
                print('Attack method: ', capec[capec['index']==num]['Execution Flow'].iloc[0])
                print('Consequences: ', capec[capec['index']==num]['Consequences'].iloc[0])
                print('Severity: ', capec[capec['index']==num]['Typical Severity'].iloc[0])
                print('Attack likelihood: ', capec[capec['index']==num]['Likelihood Of Attack'].iloc[0])
        class Ransomware(Thing):
            #@classmethod
            def capec_data(self):
                num = 549
                print('CAPEC id:', capec[capec['index']==num]['index'].iloc[0])
                print('Name: ', capec[capec['index']==num]['Name'].iloc[0])
                print('Vulnerabilties: ', capec[capec['index']==num]['Related Weaknesses'].iloc[0])
                print('Countermeasures: ', capec[capec['index']==num]['Mitigations'].iloc[0])
                print('Prerequisires: ', capec[capec['index']==num]['Prerequisites'].iloc[0])
                print('Skills needed: ', capec[capec['index']==num]['Skills Required'].iloc[0])
                print('Resources needed: ', capec[capec['index']==num]['Resources Required'].iloc[0])
                print('Attack method: ', capec[capec['index']==num]['Execution Flow'].iloc[0])
                print('Consequences: ', capec[capec['index']==num]['Consequences'].iloc[0])
                print('Severity: ', capec[capec['index']==num]['Typical Severity'].iloc[0])
                print('Attack likelihood: ', capec[capec['index']==num]['Likelihood Of Attack'].iloc[0])

    onto.save('/home/avirtekdev/sicong_bau/host_bau/ivan/preprocessing/src/files/capec_ontology.owl')
