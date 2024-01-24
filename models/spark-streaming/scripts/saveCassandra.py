#!/usr/bin/env python 

"""
Application:        DIS EVL
File name:          saveCassandra.py 
Author:             Martin Manuel Lopez
Creation:           11/22/2023

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

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
import os

class CassandraSink:
    def __init__(self, keyspace, table):
        self.keyspace = keyspace
        self.table = table
        current_dir = os.getcwd()
        self.checkpointLocation = current_dir + "/checkpoint" 

    def write(self, df):
        cassandraConfig = {
            "keyspace": self.keyspace,
            "table": self.table,
            "outputMode": "append"
        }

        df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .options(**cassandraConfig) \
            .option("checkpointLocation", self.checkpointLocation) \
            .start()

        consolefridge = df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()

        
        