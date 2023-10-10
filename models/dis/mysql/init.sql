CREATE DATABASE IF NOT EXISTS dis_db;

USE dis_db;

CREATE TABLE IF NOT EXISTS fridge(
    id int primary key AUTO_INCREMENT,
    temperature float,
    condition varchar(255),
    attack varchar(255),
    label int,
);