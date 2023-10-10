#!/bin/bash

# Connect to MySQL server in the Docker container using the container's IP address
mysql -u root -p -h 172.18.0.8 -P 3306 -e "CREATE DATABASE IF NOT EXISTS dis_db;"
mysql -u root -p -h 172.18.0.8 -P 3306 -e "USE dis_db; CREATE TABLE IF NOT EXISTS fridge(
    id int primary key AUTO_INCREMENT,
    temperature float,
    temp_condition varchar(255),
    attack varchar(255),
    label int
);"


# Change the directory to the script location
cd /home/martinmlopez/DIS_EVL/models/spark-streaming/scripts


# Submit the Spark job
spark-submit --jars jars/kafka-clients-3.4.0.jar,jars/spark-sql-kafka-0-10_2.12-3.3.0.jar,jars/spark-streaming-kafka-0-10-assembly_2.12-3.3.0.jar,jars/commons-pool2-2.11.1.jar,jars/spark-cassandra-connector_2.12-3.3.0.jar,jars/jsr166e-1.1.0.jar,jars/spark-cassandra-connector-assembly_2.12-3.3.0.jar,jars/mysql-connector-java-8.0.28.jar pyspark_streaming.py
