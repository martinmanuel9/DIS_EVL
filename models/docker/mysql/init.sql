CREATE DATABASE IF NOT EXISTS dis;

USE dis;

CREATE TABLE IF NOT EXISTS fridge_table
(device VARCHAR(255), temperature DOUBLE, \`condition\` VARCHAR(255), attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));


CREATE TABLE IF NOT EXISTS garage_table
(door_state VARCHAR(255), sphone INT, attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));

CREATE TABLE IF NOT EXISTS gps_table
(longitude DOUBLE, latitude DOUBLE, altitude DOUBLE, roll DOUBLE, pitch DOUBLE, yaw DOUBLE, attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));

CREATE TABLE IF NOT EXISTS light_table
(motion_status VARCHAR(255), light_status VARCHAR(255), attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));
CREATE TABLE IF NOT EXISTS modbus_table
(fc1 DOUBLE, fc2 DOUBLE, fc3 DOUBLE, fc4 DOUBLE, attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));
CREATE TABLE IF NOT EXISTS thermostat_table
(device VARCHAR(255), temperature DOUBLE, temp_status INT, attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));
CREATE TABLE IF NOT EXISTS weather_table
(device VARCHAR(255), temperature DOUBLE, pressure DOUBLE, humidity DOUBLE, attack VARCHAR(255), label INT, uuid CHAR(36), PRIMARY KEY (uuid));

