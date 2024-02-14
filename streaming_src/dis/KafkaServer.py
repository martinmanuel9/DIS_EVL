#!/usr/bin/env python 

"""
Application:        Apache Kafka Server
File name:          KafkaServer.py
Author:             Martin Manuel Lopez
Creation:           9/17/2023

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

from confluent_kafka import Producer, KafkaError
import subprocess
import time
import sys
import os

class KafkaServer():
    def __init__(self):
        kafka_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'kafka_2.13-3.5.0'))
        # Check if the target directory exists
        if os.path.exists(kafka_directory):
            # Change the current working directory to the target directory
            os.chdir(kafka_directory)
            print(f"Changed working directory to: {os.getcwd()}")
        else:
            print(f"Target directory '{kafka_directory}' does not exist.")
            sys.exit(1)  # Exit with an error code

    def startKafka(self):
        print("Starting Kafka and ZooKeeper...")

        # Define ZooKeeper and Kafka properties
        zookeeper_cmd = "bin/zookeeper-server-start.sh config/zookeeper.properties"
        kafka_cmd = "bin/kafka-server-start.sh config/server.properties"

        # Start ZooKeeper and Kafka subprocesses
        self.zookeeper_process = subprocess.Popen(zookeeper_cmd, shell=True)
        time.sleep(5)  # Wait for ZooKeeper to start
        kafka_process = subprocess.Popen(kafka_cmd, shell=True)
        time.sleep(5)  # Wait for Kafka to start

        try:
            print("\nZooKeeper and Kafka are running. Press Ctrl+C to stop.\n")
            kafka_process.wait()  # Wait for Kafka process to finish (manually stop with Ctrl+C)

        except KeyboardInterrupt:
            pass
        # finally:
        #     kafka.stopKafka()

    def stopKafka(self):
        # Stop the ZooKeeper process when done
        self.zookeeper_process.terminate()
        self.zookeeper_process.wait()
        print("\nZooKeeper and Kafka have been stopped.\n")

if __name__ == "__main__":
    kafka = KafkaServer()
    kafka.startKafka()
    # kafka.stopKafka()