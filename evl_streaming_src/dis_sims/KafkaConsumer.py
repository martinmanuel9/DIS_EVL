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
import pyarrow.parquet as pq

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
        self.kafka_train_data = {topic: pd.DataFrame() for topic in self.topic}
    
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
                            
                            if self.kafka_train_data["gps"].empty:
                                self.kafka_train_data["gps"] = gps_df
                            else:
                                self.kafka_train_data["gps"] = pd.concat([self.kafka_train_data["gps"], gps_df], ignore_index=True)
                            
                            if self.mode == "test":
                                return gps_df
                        
                        elif pdu.pduType == 73: # Light
                            light_data = {
                                "pdu_id": pdu.pduType,
                                "pdu_name": pduTypeName,
                                "light_status": pdu.light_status,
                                "attack": pdu.attack,
                                "label": pdu.label
                                
                            }
                            light_df = pd.DataFrame([light_data])
                            
                            if self.verbose == "true":
                                print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                    + " Motion Status : {}\n".format(pdu.motion_status)
                                    + " Light Status  : {}\n".format(pdu.light_status)
                                    + " Attack        : {}\n".format(pdu.attack)
                                    + " Label         : {}\n".format(pdu.label)
                                    )
                            
                            if self.kafka_train_data['light'].empty:
                                self.kafka_train_data["light"] = light_df
                            else:
                                self.kafka_train_data["light"] = pd.concat([self.kafka_train_data["light"], light_df], ignore_index=True)
                            if self.mode == "test":
                                return light_df
                        
                        elif pdu.pduType == 70:  # environment
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
                                if self.kafka_train_data['fridge'].empty:
                                    self.kafka_train_data["fridge"] = fridge_df
                                else:
                                    self.kafka_train_data["fridge"] = pd.concat([self.kafka_train_data["fridge"], fridge_df], ignore_index=True)
                                
                                if self.mode == "test":
                                    return fridge_df
                            
                            if pdu.device == "Thermostat":
                                if self.kafka_train_data['thermostat'].empty:
                                    self.kafka_train_data["thermostat"] = thermostat_df
                                else:
                                    self.kafka_train_data["thermostat"] = pd.concat([self.kafka_train_data["thermostat"], thermostat_df], ignore_index=True)
                                
                                if self.mode == "test":
                                    return thermostat_df
                            
                            if pdu.device == "Weather":
                                if self.kafka_train_data['weather'].empty:
                                    self.kafka_train_data["weather"] = weather_df
                                else:
                                    self.kafka_train_data["weather"] = pd.concat([self.kafka_train_data["weather"], weather_df], ignore_index=True)
                                
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
                            
                            if self.verbose == "true":
                                print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                    + " FC1 Register    : {}\n".format(pdu.fc1)
                                    + " FC2 Discrete    : {}\n".format(pdu.fc2)
                                    + " FC3 Register    : {}\n".format(pdu.fc3)
                                    + " FC4 Read Coil   : {}\n".format(pdu.fc4)
                                    + " Attack          : {}\n".format(pdu.attack)
                                    + " Label           : {}\n".format(pdu.label)
                                    )
                            
                            if self.kafka_train_data['modbus'].empty:
                                self.kafka_train_data["modbus"] = modbus_df
                            else:
                                self.kafka_train_data["modbus"] = pd.concat([self.kafka_train_data["modbus"], modbus_df], ignore_index=True)
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
                            
                            if self.verbose == "true":
                                print("Received {}: {} Bytes\n".format(pduTypeName, len(message), flush=True)
                                    + " Door State: {}\n".format(pdu.door_state)
                                    + " SPhone: {}\n".format(pdu.sphone)
                                    + " Attack: {}\n".format(pdu.attack)
                                    + " Label : {}\n".format(pdu.label)
                                    )
                            
                            if self.kafka_train_data['garage'].empty:
                                self.kafka_train_data["garage"] = garage_df
                            else:
                                self.kafka_train_data["garage"] = pd.concat([self.kafka_train_data["garage"], garage_df], ignore_index=True)
                            if self.mode == "test":
                                return garage_df
                        
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

    def consume_messages(self, timeout = 10):
        last_message_time = time.time()
        # messages = []
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                self.on_message(msg)
                # messages.append(msg)
                last_message_time = time.time() # update last message time
                if time.time() - last_message_time > timeout:
                    table = pa.Table.from_pandas(self.kafka_train_data)
                    parquet_file_path = "../datasets"
                    pq.write_table(table, parquet_file_path)
                    print("Timeout: No messages received in the last {} seconds".format(timeout))
                    # self.consumer.close()
                    last_message_time = time.time() # reset last message time
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(f"Error consuming message: {e}")
            
        
    def train(self, verbose):
        try:
            logging.basicConfig(level=logging.INFO)
            
            threads = []
            
            fridgeTrainConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["fridge"],
                                transmission="kafka_pdu",
                                mode="train",
                                verbose=verbose
                                )
            
            garageTrainConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["garage"],
                                transmission="kafka_pdu",
                                mode="train",
                                verbose=verbose
                                )
            
            gpsTrainConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["gps"],
                                transmission="kafka_pdu",
                                mode="train",
                                verbose=verbose
                                )
            
            modbusTrainConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["modbus"],
                                transmission="kafka_pdu",
                                mode="train",
                                verbose=verbose
                                )
                                
            thermostatTrainConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["thermostat"],
                                transmission="kafka_pdu",
                                mode="train",
                                verbose=verbose
                                )
            
            weatherTrainConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["weather"],
                                transmission="kafka_pdu",
                                mode="train",
                                verbose=verbose
                                )
                                
            # create thread for each topic
            threads.append(threading.Thread(target=fridgeTrainConsumer.consume_messages))
            threads.append(threading.Thread(target=garageTrainConsumer.consume_messages))
            threads.append(threading.Thread(target=gpsTrainConsumer.consume_messages))  
            threads.append(threading.Thread(target=modbusTrainConsumer.consume_messages))
            threads.append(threading.Thread(target=thermostatTrainConsumer.consume_messages))
            threads.append(threading.Thread(target=weatherTrainConsumer.consume_messages))
            
            for thread in threads:
                thread.start()
                
            # wait for all threads to finish
            for thread in threads:
                thread.join()
                if any(thread.is_alive() for thread in threads):
                    # There is still processing happening, do nothing
                    pass
                else:
                    # Stop all threads
                    for thread in threads:
                        thread.stop()
            
            print("training complete\n", self.kafka_train_data)
            self.consumer.close()  # close the Kafka consumer connection

        except Exception as e:
            print(f"Error: {e}")
            print("Error: Could not run kafka during training data.")
            return False

            
    def test(self, verbose):
        try:
            logging.basicConfig(level=logging.INFO)
            
            threads = []
            
            fridgeTestConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["fridge"],
                                transmission="kafka_pdu",
                                mode="test",
                                verbose=verbose
                                )
            
            garageTestConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["garage"],
                                transmission="kafka_pdu",
                                mode="test",
                                verbose=verbose
                                )
            
            gpsTestConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["gps"],
                                transmission="kafka_pdu",
                                mode="test",
                                verbose=verbose
                                )
            
            modbusTestConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["modbus"],
                                transmission="kafka_pdu",
                                mode="test",
                                verbose=verbose
                                )
                                
            thermostatTestConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["thermostat"],
                                transmission="kafka_pdu",
                                mode="test",
                                verbose=verbose
                                )
            
            weatherTestConsumer = KafkaConsumer(bootstrap_servers="172.18.0.4:9092",
                                group_id="dis",
                                topic= ["weather"],
                                transmission="kafka_pdu",
                                mode="test",
                                verbose=verbose
                                )
                                
            # create thread for each topic
            threads.append(threading.Thread(target=fridgeTestConsumer.consume_messages))
            threads.append(threading.Thread(target=garageTestConsumer.consume_messages))
            threads.append(threading.Thread(target=gpsTestConsumer.consume_messages))  
            threads.append(threading.Thread(target=modbusTestConsumer.consume_messages))
            threads.append(threading.Thread(target=thermostatTestConsumer.consume_messages))
            threads.append(threading.Thread(target=weatherTestConsumer.consume_messages))
            
            for thread in threads:
                thread.start()
                
            # wait for all threads to finish
            for thread in threads:
                thread.join()
                
            
            print("testing complete\n")
            self.consumer.close()  # close the Kafka consumer connection


        except Exception as e:
            print(f"Error: {e}")
            print("Error: Could not run kafka during validating data.")
            return False


# def main():
#     try:
#         while True:
#             logging.basicConfig(level=logging.INFO)
#             parser = argparse.ArgumentParser(description="Kafka Consumer Training")
#             parser.add_argument("--bootstrap_servers", default="172.18.0.4:9092", help="Bootstrap servers")
#             parser.add_argument("--group_id", default="dis", help="Group ID")
#             parser.add_argument("--topic", nargs="+", default=["fridge", "garage", "gps", "light", "modbus", "thermostat", "weather"], help="Topic")
#             parser.add_argument("--transmission", choices = ["kafka","kafka_pdu"], default="kafka_pdu", help="Transmission option")
#             parser.add_argument("--mode", choices=["train", "test"], default="train", help="Mode: train or test")
#             parser.add_argument("--verbose", choices=["false", "true"], default="false", help="Enable verbose mode")

#             args = parser.parse_args() 

#             consumer = KafkaConsumer(args.bootstrap_servers, args.group_id, args.topic, args.transmission, args.mode, args.verbose)
#             consumer.consume_messages()
#             time.sleep(1)  # delay of 1 second

#     except KeyboardInterrupt:
#         consumer.close()

# if __name__ == "__main__":
#     main()
    