#!/bin/bash

# Set the MySQL password in a variable
password="secret"

# Connect to MySQL server in the Docker container using the container's IP address
mysql -u root -p$password  -h 172.18.0.8 -P 3306 -e "CREATE DATABASE IF NOT EXISTS dis_db;"
mysql -u root -p$password  -h 172.18.0.8 -P 3306 -e "USE dis_db; CREATE TABLE IF NOT EXISTS fridge(
    id int primary key AUTO_INCREMENT,
    device varchar(255),
    temperature float,
    temp_condition varchar(255),
    attack varchar(255),
    label int
);"


# Define your Cassandra container name
CASSANDRA_CONTAINER_NAME="cassandra"

# Start the Cassandra container if not already running
docker start $CASSANDRA_CONTAINER_NAME 2>/dev/null

# Run CQLSH commands in the Cassandra containerCRE
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE KEYSPACE IF NOT EXISTS dis WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.fridge_table(device TEXT, temperature DOUBLE, condition TEXT, attack TEXT, label INTEGER, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.garage_table(door_state TEXT, sphone INTEGER, attack TEXT, label INTEGER, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.gps_table(longitude DOUBLE, latitude DOUBLE, altitude DOUBLE, roll DOUBLE, pitch DOUBLE, yaw DOUBLE, attack TEXT, label INTEGER, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.light_table(motion_status TEXT, light_status TEXT, attack TEXT, label INTEGER, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.modbus_table(fc1 DOUBLE, fc2 DOUBLE, fc3 DOUBLE, fc4 DOUBLE, attack TEXT, label INTEGER, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.thermostat_table(device TEXT, temperature DOUBLE, temp_status INTEGER, attack TEXT, label INTEGER, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.weather_table(device TEXT, temperature DOUBLE, pressure DOUBLE, humidity DOUBLE, attack TEXT, label INTEGER, uuid UUID, PRIMARY KEY (uuid));"

# Check the exit status of cqlsh
if [ $? -eq 0 ]; then
    echo "CQL commands executed successfully."
else
    echo "Error executing CQL commands."
fi


docker cp /home/martinmlopez/DIS_EVL/models spark_master:/opt/bitnami/spark/tmp