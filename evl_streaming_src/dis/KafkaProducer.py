#!/usr/bin/env python 

"""
Application:        Apache Kafka Producer  
File name:          KafkaProducer.py
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

from confluent_kafka import Producer
import socket
import time
import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class KafkaProducer:
    def __init__(self, bootstrap_servers, topic):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.topic = topic

    def produce_message(self, message):
        try:
            self.producer.produce(self.topic, key=None, value=message)
            self.producer.flush()
            # print(f"Produced message: {message}")
            print(f"Produced message via Kakfa Producer")
        except Exception as e:
            print(f"Error producing message: {e}")

    

# if __name__ == "__main__":
#     producer = KafkaProducer('localhost:9092', 'my-topic')
#     producer.produce_message('Hello, Kafka! This is me running kafka')
#     producer.produce_message('Hello, Kafka! This is Marty Manny')
#     del producer
    

