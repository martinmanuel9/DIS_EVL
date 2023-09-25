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

# MIT License
#
# Copyright (c) 2023
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

import argparse
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

class KafkaConsumer:
    def __init__(self, bootstrap_servers, group_id, topic, tranmission):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.topic = topic
        self.consumer.subscribe([self.topic])
        self.transmission = tranmission

    def consume_messages(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
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
                                memoryStream = BytesIO(message)
                                data = memoryStream.getvalue()
                                if pdu.pduType == 1: # PduTypeDecoders.EntityStatePdu:
                                    gps = GPS()
                                    loc = (pdu.entityLocation.x,
                                            pdu.entityLocation.y,
                                            pdu.entityLocation.z,
                                            pdu.entityOrientation.psi,
                                            pdu.entityOrientation.theta,
                                            pdu.entityOrientation.phi
                                            )
                                    
                                    body = gps.ecef2llarpy(*loc)
                                    print("Received {}: {} Bytes\n".format(pduTypeName, len(data), flush=True)
                                            + " Id          : {}\n".format(pdu.entityID.entityID)
                                            + " Latitude    : {:.2f} degrees\n".format(rad2deg(body[0]))
                                            + " Longitude   : {:.2f} degrees\n".format(rad2deg(body[1]))
                                            + " Altitude    : {:.0f} meters\n".format(body[2])
                                            + " Yaw         : {:.2f} degrees\n".format(rad2deg(body[3]))
                                            + " Pitch       : {:.2f} degrees\n".format(rad2deg(body[4]))
                                            + " Roll        : {:.2f} degrees\n".format(rad2deg(body[5]))
                                            + " Attack      : {}\n".format(pdu.attack.decode('utf-8'))
                                            + " Label       : {}\n".format(pdu.label)
                                            )
                                
                                elif pdu.pduType == 73: # Light
                                    print("Received {}: {} Bytes\n".format(pduTypeName, len(data), flush=True)
                                        + " Motion Status : {}\n".format(pdu.motion_status)
                                        + " Light Status  : {}\n".format(pdu.light_status.decode('utf-8'))
                                        + " Attack        : {}\n".format(pdu.attack.decode('utf-8'))
                                        + " Label         : {}\n".format(pdu.label)
                                        )
                                
                                elif pdu.pduType == 70:  # environment
                                    print("Received {}: {} Bytes \n".format(pduTypeName, len(data), flush=True)
                                            + " Device      : {}\n".format(pdu.device.decode('utf-8'))
                                            + " Temperature : {}\n".format(pdu.temperature)
                                            + " Pressure    : {}\n".format(pdu.pressure)
                                            + " Humidity    : {}\n".format(pdu.humidity)
                                            + " Condition   : {}\n".format(pdu.condition.decode('utf-8'))
                                            + " Temp Status : {}\n".format(pdu.temp_status)
                                            + " Attack      : {}\n".format(pdu.attack.decode('utf-8'))
                                            + " Label       : {}\n".format(pdu.label)  
                                            )
                                    
                                elif pdu.pduType == 71: # modbus
                                    print("Received {}: {} Bytes\n".format(pduTypeName, len(data), flush=True)
                                        + " FC1 Register    : {}\n".format(pdu.fc1)
                                        + " FC2 Discrete    : {}\n".format(pdu.fc2)
                                        + " FC3 Register    : {}\n".format(pdu.fc3)
                                        + " FC4 Read Coil   : {}\n".format(pdu.fc4)
                                        + " Attack          : {}\n".format(pdu.attack.decode('utf-8'))
                                        + " Label           : {}\n".format(pdu.label)
                                        )
                                
                                elif pdu.pduType == 72: # garage
                                    print("Received {}: {} Bytes\n".format(pduTypeName, len(data), flush=True)
                                        + " Door State: {}\n".format(pdu.door_state.decode('utf-8'))
                                        + " SPhone: {}\n".format(pdu.sphone)
                                        + " Attack: {}\n".format(pdu.attack.decode('utf-8'))
                                        + " Label : {}\n".format(pdu.label)
                                        )
                                else: 
                                    print("Received PDU {}, {} bytes".format(pduTypeName, len(data)), flush=True)
                            
                            #------ Regular Kafka Messages ------#
                            else:
                                message = message.decode('utf-8')
                                logging.info(f"Received message: {message}")

                        except UnicodeDecodeError as e:
                            print("UnicodeDecodeError: ", e)
                    else:
                        logging.error("Received message is not a byte-like object.")
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(f"Error consuming message: {e}")


    def close(self):
        self.consumer.close()

def main():
    try:
        while True:
            logging.basicConfig(level=logging.INFO)
            parser = argparse.ArgumentParser(description="Kafka Consumer")
            parser.add_argument("--bootstrap_servers", default="localhost:9092", help="Bootstrap servers")
            parser.add_argument("--group_id", default="dis", help="Group ID")
            parser.add_argument("--topic", default="dis", help="Topic")
            parser.add_argument("--transmission", default="kafka_pdu", help="Transmission option")

            args = parser.parse_args()

            consumer = KafkaConsumer(args.bootstrap_servers, args.group_id, args.topic, args.transmission)
            consumer.consume_messages()
            # time.sleep(1)  # delay of 1 second

    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()
            