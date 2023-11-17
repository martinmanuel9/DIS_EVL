#!/bin/bash

# MySQL Configuration
MYSQL_CONTAINER_NAME="mysql"
MYSQL_USER="root"
MYSQL_PASSWORD="secret"
MYSQL_DATABASE="dis"

# Cassandra Configuration
CASSANDRA_CONTAINER_NAME="cassandra"

# Function to execute SQL commands for MySQL
first_execute_sql() {
    docker exec -i $MYSQL_CONTAINER_NAME mysql -u$MYSQL_USER -p$MYSQL_PASSWORD -e "$1"
}


first_execute_sql "CREATE DATABASE IF NOT EXISTS $MYSQL_DATABASE;"
first_execute_sql "USE $MYSQL_DATABASE;"

execute_sql() {
    docker exec -i $MYSQL_CONTAINER_NAME mysql -u$MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE -e "$1"
}

execute_sql "CREATE TABLE IF NOT EXISTS fridge_table(device VARCHAR(255), temperature DOUBLE, \`condition\` VARCHAR(255), attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));"
execute_sql "CREATE TABLE IF NOT EXISTS garage_table(door_state VARCHAR(255), sphone INT, attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));"
execute_sql "CREATE TABLE IF NOT EXISTS gps_table(longitude DOUBLE, latitude DOUBLE, altitude DOUBLE, roll DOUBLE, pitch DOUBLE, yaw DOUBLE, attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));"
execute_sql "CREATE TABLE IF NOT EXISTS light_table(motion_status VARCHAR(255), light_status VARCHAR(255), attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));"
execute_sql "CREATE TABLE IF NOT EXISTS modbus_table(fc1 DOUBLE, fc2 DOUBLE, fc3 DOUBLE, fc4 DOUBLE, attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));"
execute_sql "CREATE TABLE IF NOT EXISTS thermostat_table(device VARCHAR(255), temperature DOUBLE, temp_status INT, attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));"
execute_sql "CREATE TABLE IF NOT EXISTS weather_table(device VARCHAR(255), temperature DOUBLE, pressure DOUBLE, humidity DOUBLE, attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));"



# Cassandra Section
docker start $CASSANDRA_CONTAINER_NAME 2>/dev/null
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE KEYSPACE IF NOT EXISTS dis WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.fridge_table(device TEXT, temperature DOUBLE, condition TEXT, attack TEXT, label INT, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.garage_table(door_state TEXT, sphone INT, attack TEXT, label INT, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.gps_table(longitude DOUBLE, latitude DOUBLE, altitude DOUBLE, roll DOUBLE, pitch DOUBLE, yaw DOUBLE, attack TEXT, label INT, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.light_table(motion_status TEXT, light_status TEXT, attack TEXT, label INT, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.modbus_table(fc1 DOUBLE, fc2 DOUBLE, fc3 DOUBLE, fc4 DOUBLE, attack TEXT, label INT, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.thermostat_table(device TEXT, temperature DOUBLE, temp_status INT, attack TEXT, label INT, uuid UUID, PRIMARY KEY (uuid));"
docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.weather_table(device TEXT, temperature DOUBLE, pressure DOUBLE, humidity DOUBLE, attack TEXT, label INT, uuid UUID, PRIMARY KEY (uuid));"

# Check the exit status of cqlsh
if [ $? -eq 0 ]; then
    echo "CQL commands executed successfully."
else
    echo "Error executing CQL commands."
fi

# Rest of your script (assuming it's related to Spark and copying directories)
# current_directory=$(pwd)
# parent_directory=$(dirname "$current_directory")
# echo "Directory to copy onto spark_master: $parent_directory"

# if [ -d "$parent_directory" ]; then
#     docker cp "$parent_directory" spark_master:/opt/bitnami/spark/tmp
#     echo "Directory copied successfully."
# else
#     echo "Error: The directory $parent_directory does not exist."
# fi
