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

UDP_PORT = 3001

udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udpSocket.bind(("", UDP_PORT))

class KafkaConsumer:
    def __init__(self, bootstrap_servers, group_id, topic):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.topic = topic
        self.consumer.subscribe([self.topic])

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

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    bootstrap_servers = 'localhost:9092'
    group_id = 'dis'
    topic = 'dis'

    consumer = KafkaConsumer(bootstrap_servers, group_id, topic)
    consumer.consume_messages()
    # consumer.close()

    
            