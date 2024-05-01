#!/usr/bin/env python 

"""
Application:        Apache Kafka Consumer  
File name:          KafkaConsumer.py
Author:             Martin Manuel Lopez
Creation:           9/14/2023

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

import argparse
import threading
from confluent_kafka import Consumer, KafkaError
import socket
import time
import sys
import os
import json
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from opendismodel.opendis.dis7 import *
from opendismodel.opendis.PduFactory import createPdu
from opendismodel.opendis.RangeCoordinates import *
from io import BytesIO
import pandas as pd
import pyarrow as pa
import datetime
import pyarrow.parquet as pq
from category_encoders import OrdinalEncoder 

class KafkaConsumer:
    def __init__(self, bootstrap_servers, group_id, topic, transmission, mode, verbose):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Disable auto-commit of offsets
            'enable.auto.offset.store': False,  # Disable automatic offset storage
            'enable.partition.eof': False  # Disable automatic partition EOF event
        })
        self.topic = topic
        self.consumer.subscribe(self.topic)
        self.transmission = transmission
        self.mode = mode
        self.verbose = verbose
        self.lock = threading.Lock() 
        self.kafka_train_data = {topic: pd.DataFrame() for topic in self.topic}
        
    def update_data(self, topic, value):
        with self.lock:
            if self.kafka_train_data[topic].empty:
                self.kafka_train_data[topic] = value
            else:
                self.kafka_train_data[topic] = pd.concat([self.kafka_train_data[topic], value], ignore_index=True)

    
    def on_message(self, msg):
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
        else:
            message = msg.value()
            if isinstance(message, bytes):
                try:
                    # ------- Sending PDUs via Kafka -------#
                    if self.transmission == 'kafka_pdu':
                        pdu = createPdu(message)
                        pduTypeName = pdu.__class__.__name__
                        
                        if pdu.pduType == 1: # PduTypeDecoders.EntityStatePdu:
                            # Aggregate GPS data
                            gps_data = {
                                "pdu_id": pdu.pduType,
                                "pdu_name": pduTypeName,
                                "longitude": pdu.entityLocation.x,
                                "latitude": pdu.entityLocation.y,
                                "altitude": pdu.entityLocation.z,
                                "yaw": pdu.entityOrientation.psi,
                                "pitch": pdu.entityOrientation.theta,
                                "roll": pdu.entityOrientation.phi,
                                "attack": pdu.attack,
                                "label": pdu.label
                            }
                            gps_df = pd.DataFrame([gps_data])
                            
                            gps_mapping = [
                                {'col': 'pdu_name', 'mapping': {'EntityStatePdu': 0}},
                                {'col': 'attack', 'mapping': {'normal': 0, 'backdoor': 1, 'ddos': 2, 'injection': 3, 'password': 4, 'ransomeware': 5, 'scanning': 6, 'xss': 7}}]
                            gps_df= OrdinalEncoder(cols=['pdu_name', 'attack'], mapping=gps_mapping).fit(gps_df).transform(gps_df)
            
                            
                            if self.verbose == "true":
                                print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                        + " PDU_Id      : {}\n".format(pdu.pduType)
                                        + " Longitude   : {:.3f} degrees\n".format(pdu.entityLocation.x)
                                        + " Latitude    : {:.3f} degrees\n".format(pdu.entityLocation.y)
                                        + " Altitude    : {:.3f} meters\n".format(pdu.entityLocation.z)
                                        + " Yaw         : {:.3f} degrees\n".format(pdu.entityOrientation.psi)
                                        + " Pitch       : {:.3f} degrees\n".format(pdu.entityOrientation.theta)
                                        + " Roll        : {:.3f} degrees\n".format(pdu.entityOrientation.phi)
                                        + " Attack      : {}\n".format(pdu.attack)
                                        + " Label       : {}\n".format(pdu.label)
                                        )
                            
                            self.update_data("gps", gps_df)
                            
                            if self.mode == "test":
                                return gps_df
                        
                        
                        
                        elif pdu.pduType == 70:  # environment
                            # map the environment devices 
                            # Fridge: 0, Thermostat: 1, Weather: 2
                            if pdu.device == "Fridge":
                                fridge_data = {
                                    "pdu_id": pdu.pduType,
                                    "pdu_name": pduTypeName,
                                    "device": pdu.device,
                                    "temperature": pdu.temperature,
                                    "condition": pdu.condition,
                                    "attack" : pdu.attack,
                                    "label": pdu.label
                                }
                                
                                fridge_df = pd.DataFrame([fridge_data])
                                # ## fridge mapping
                                fridge_mapping = [{'col': 'condition', 'mapping' : {"low": 1, "high": 2}},
                                                {'col': 'pdu_name', 'mapping': {'Environment': 1}}, 
                                                {'col': 'device', 'mapping': {'Fridge': 0}},
                                                {'col': 'attack', 'mapping': {'normal': 0, 'backdoor': 1, 'ddos': 2, 'injection': 3, 'password': 4, 'ransomware': 5, 'scanning': 6, 'xss': 7 }}] 
                                fridge_df = OrdinalEncoder(cols=['condition', 'pdu_name', 'device', 'attack'], mapping=fridge_mapping).fit(fridge_df).transform(fridge_df)
                                
                            
                            if pdu.device == "Thermostat":
                                thermostat_data = {
                                    "pdu_id": pdu.pduType,
                                    "pdu_name": pduTypeName,
                                    "device": pdu.device,
                                    "temperature": pdu.temperature,
                                    "temp_status": pdu.temp_status,
                                    "attack" : pdu.attack,
                                    "label": pdu.label
                                }
                                
                                thermostat_df = pd.DataFrame([thermostat_data])
                                
                                ## thermostat mapping
                                thermo_mapping = [ 
                                    {'col': 'pdu_name', 'mapping': {'Environment': 1}},
                                    {'col': 'device', 'mapping': {'Thermostat': 1}},
                                    {'col': 'attack', 'mapping':{'normal': 0, 'backdoor': 1, 'injection': 3, 'password': 4, 'ransomeware': 5, 'scanning': 6, 'xss': 7 }}]
                                thermostat_df = OrdinalEncoder(cols=['pdu_name', 'device', 'attack'], mapping=thermo_mapping).fit(thermostat_df).transform(thermostat_df)
            
                            
                            if pdu.device == "Weather":
                                weather_data = {
                                    "pdu_id": pdu.pduType,
                                    "pdu_name": pduTypeName,
                                    "device": pdu.device,
                                    "temperature": pdu.temperature,
                                    "pressure": pdu.pressure,
                                    "humidity": pdu.humidity,
                                    "attack" : pdu.attack,
                                    "label": pdu.label
                                }
                                
                                weather_df = pd.DataFrame([weather_data])
                                ## weather mapping
                                weather_mapping = [
                                    {'col': 'pdu_name', 'mapping': {'Environment': 1}},
                                    {'col': 'device', 'mapping': {'Weather': 2}},
                                    {'col': 'attack', 'mapping':{'normal': 0, 'backdoor': 1, 'ddos': 2, 'injection': 3, 'password': 4, 'ransomeware': 5, 'scanning': 6, 'xss': 7 }}]
                                weather_df = OrdinalEncoder(cols=['pdu_name', 'device','attack'], mapping=weather_mapping).fit(weather_df).transform(weather_df) 
                            
                            if self.verbose == "true":
                                print("Received {}: {} Bytes \n".format(pduTypeName, len(message), flush=True)
                                        + " Device      : {}\n".format(pdu.device)
                                        + " Temperature : {}\n".format(pdu.temperature)
                                        + " Pressure    : {}\n".format(pdu.pressure)
                                        + " Humidity    : {}\n".format(pdu.humidity)
                                        + " Condition   : {}\n".format(pdu.condition)
                                        + " Temp Status : {}\n".format(pdu.temp_status)
                                        + " Attack      : {}\n".format(pdu.attack)
                                        + " Label       : {}\n".format(pdu.label)  
                                        )
                            if pdu.device == "Fridge":
                                self.update_data("fridge", fridge_df)
                                
                                if self.mode == "test":
                                    return fridge_df
                            
                            if pdu.device == "Thermostat":
                                self.update_data("thermostat", thermostat_df)
                                if self.mode == "test":
                                    return thermostat_df
                            
                            if pdu.device == "Weather":
                                self.update_data("weather", weather_df)
                                if self.mode == "test":
                                    return weather_df
                            
                            
                        elif pdu.pduType == 71: # modbus
                            modbus_data = {
                                "pdu_id": pdu.pduType,
                                "pdu_name": pduTypeName,
                                "fc1": pdu.fc1,
                                "fc2": pdu.fc2,
                                "fc3": pdu.fc3,
                                "fc4": pdu.fc4,
                                "attack": pdu.attack,
                                "label": pdu.label
                            }
                            modbus_df = pd.DataFrame([modbus_data])
                            ## modbus mapping
                            modbus_mapping = [
                                {'col': 'pdu_name', 'mapping': {'Modbus': 2}},
                                {'col': 'attack', 'mapping': {'normal': 0, 'backdoor': 1, 'injection': 3, 'password': 4, 'scanning': 6, 'xss': 7}}]
                            modbus_df = OrdinalEncoder(cols=['pdu_name', 'attack'], mapping=modbus_mapping).fit(modbus_df).transform(modbus_df)
            
                            
                            if self.verbose == "true":
                                print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                    + " FC1 Register    : {}\n".format(pdu.fc1)
                                    + " FC2 Discrete    : {}\n".format(pdu.fc2)
                                    + " FC3 Register    : {}\n".format(pdu.fc3)
                                    + " FC4 Read Coil   : {}\n".format(pdu.fc4)
                                    + " Attack          : {}\n".format(pdu.attack)
                                    + " Label           : {}\n".format(pdu.label)
                                    )
                            
                            self.update_data("modbus", modbus_df)
                                
                            if self.mode == "test":
                                return modbus_df
                        
                        elif pdu.pduType == 72: # garage
                            garage_data = {
                                "pdu_id": pdu.pduType,
                                "pdu_name": pduTypeName,
                                "door_state": pdu.door_state,
                                "sphone": pdu.sphone,
                                "attack": pdu.attack,
                                "label": pdu.label
                            }
                            garage_df = pd.DataFrame(garage_data)
                            garage_mapping = [
                                    {'col': 'pdu_name', 'mapping': {'Garage': 3}},
                                    {'col': 'door_state', 'mapping': {'closed': 0, 'open': 1}},
                                    {'col': 'sphone', 'mapping': {'false  ': 0, 'true  ': 1, '0': 0, '1':1}},
                                    {'col': 'attack', 'mapping': {'normal': 0, 'backdoor': 1, 'ddos': 2, 'injection': 3, 'password': 4, 'ransomeware': 5, 'scanning': 6, 'xss': 7}}]
                            garage_df = OrdinalEncoder(cols=['pdu_name', 'door_state', 'sphone', 'attack'], mapping=garage_mapping).fit(garage_df).transform(garage_df)
                            
                            if self.verbose == "true":
                                print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                    + " Door State: {}\n".format(pdu.door_state)
                                    + " SPhone: {}\n".format(pdu.sphone)
                                    + " Attack: {}\n".format(pdu.attack)
                                    + " Label : {}\n".format(pdu.label)
                                    )
                            
                            self.update_data("garage", garage_df)
                            
                            if self.mode == "test":
                                return garage_df
                        
                        elif pdu.pduType == 73: # Light
                            light_data = {
                                "pdu_id": pdu.pduType,
                                "pdu_name": pduTypeName,
                                "light_status": pdu.light_status,
                                "attack": pdu.attack,
                                "label": pdu.label
                                
                            }
                            light_df = pd.DataFrame([light_data])
                            ## light mapping
                            light_mapping = [
                                            {'col':'pdu_name', 'mapping': {'Light': 4}}, 
                                            {'col':'light_status', 'mapping': {' off': 0, ' on': 1}}, 
                                            {'col': 'attack', 'mapping':{'normal': 0, 'backdoor': 1, 'ddos': 2, 'injection': 3, 'password': 4, 'ransomeware': 5, 'scanning': 6, 'xss': 7 }}]
                            light_df = OrdinalEncoder(cols=['pdu_name', 'light_status', 'attack'], mapping = light_mapping).fit(light_df).transform(light_df)
            
                            
                            if self.verbose == "true":
                                print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                    + " Motion Status : {}\n".format(pdu.motion_status)
                                    + " Light Status  : {}\n".format(pdu.light_status)
                                    + " Attack        : {}\n".format(pdu.attack)
                                    + " Label         : {}\n".format(pdu.label)
                                    )
                            
                            self.update_data("light", light_df)
                            
                            if self.mode == "test":
                                return light_df
                        
                        else: 
                            pduData = {
                                "pdu_id": pdu.pduType,
                                "pdu_name": pduTypeName
                            }
                            pduDf = pd.DataFrame(pduData)
                            if self.verbose == "true":
                                print("Received PDU {}, {} bytes".format(pduTypeName, len(message)), flush=True)
                                
                            return pduDf
                    
                    # ------ Regular Kafka Messages ------#
                    else:
                        message = message.decode('utf-8')
                        logging.info(f"Received message: {message}")
                    
                    # --- Commit the offset manually --- #
                    self.consumer.commit(msg)
                
                except UnicodeDecodeError as e:
                    print("UnicodeDecodeError: ", e)
            else:
                logging.error("Received message is not a byte-like object.")

    def consume_messages(self, timeout=10):
        last_message_time = time.time()
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    if time.time() - last_message_time > timeout:
                        print("Timeout: No messages received in the last {} seconds".format(timeout))
                        break  # exit the loop after timeout
                    else:
                        continue
                self.on_message(msg)
                last_message_time = time.time()  # update last message time
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(f"Error consuming message: {e}")
        finally:
            self.consumer.close()

    def merge_dicts(dict1, dict2):
        merged_dict = dict1.copy()
        for key, value in dict2.items():
            if key in merged_dict:
                merged_dict[key] = pd.concat([merged_dict[key], value])
            else:
                merged_dict[key] = value
                
        return merged_dict
        
    def train(self, verbose):
        try:
            logging.basicConfig(level=logging.INFO)
            
            threads = []
            consumers = []

            # Define Kafka consumer instances
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["fridge"], "kafka_pdu", "train", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["garage"], "kafka_pdu", "train", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["gps"], "kafka_pdu", "train", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["light"], "kafka_pdu", "train", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["modbus"], "kafka_pdu", "train", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["thermostat"], "kafka_pdu", "train", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["weather"], "kafka_pdu", "train", verbose))

            # Create threads for each consumer
            for consumer in consumers:
                threads.append(threading.Thread(target=consumer.consume_messages))

            for thread in threads:
                thread.start()
                
            # wait for all threads to finish
            for thread in threads:
                thread.join()
            
            # Merge data from all consumers
            merged_data = {}
            for consumer in consumers:
                merged_data = KafkaConsumer.merge_dicts(merged_data, consumer.kafka_train_data)
            
            print("\n\nTraining complete\n")
            
            # Save data to parquet file onto the datasets directory
            # for key, value in merged_data.items():
            #     if not value.empty:
            #         value.to_parquet(f"datasets/{key}_{datetime.datetime.now().strftime('%Y%m%d')}.parquet", index=False)
            #         print(f"Saved {key} data to datasets/{key}.parquet")
            #     else:
            #         print(f"No data received for {key}")

            return merged_data

        except Exception as e:
            print(f"Error: {e}: exiting training mode.")
            return False

    def test(self, verbose):
        try:
            logging.basicConfig(level=logging.INFO)
            
            threads = []
            consumers = []

            # Define Kafka consumer instances
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["fridge"], "kafka_pdu", "test", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["garage"], "kafka_pdu", "test", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["gps"], "kafka_pdu", "test", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["light"], "kafka_pdu", "test", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["modbus"], "kafka_pdu", "test", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["thermostat"], "kafka_pdu", "test", verbose))
            consumers.append(KafkaConsumer("172.18.0.4:9092", "dis", ["weather"], "kafka_pdu", "test", verbose))

            # Create threads for each consumer
            for consumer in consumers:
                threads.append(threading.Thread(target=consumer.consume_messages))

            for thread in threads:
                thread.start()
                
            # wait for all threads to finish
            for thread in threads:
                thread.join()
            
            # Merge data from all consumers
            merged_data = {}
            for consumer in consumers:
                merged_data = KafkaConsumer.merge_dicts(merged_data, consumer.kafka_train_data)
            
            print("\n\nTesting complete\n")

            return merged_data

        except Exception as e:
            print(f"Error: {e}: exiting training mode.")
            return False
