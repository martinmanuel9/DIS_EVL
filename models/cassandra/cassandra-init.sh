#!/usr/bin/env bash

# Specify the Cassandra host and port
CASSANDRA_HOST="cassandra"
CASSANDRA_PORT="9042"

# Function to check if Cassandra is available
wait_for_cassandra() {
    until nc -z "$CASSANDRA_HOST" "$CASSANDRA_PORT"; do
        sleep 5
        echo "Waiting for Cassandra..."
    done
}

# Wait for Cassandra to become available
wait_for_cassandra

# Create keyspace and table
echo "Creating keyspace and table..."
cqlsh "$CASSANDRA_HOST" -u cassandra -p cassandra -e "CREATE KEYSPACE IF NOT EXISTS dis WITH replication = {'class': 'disclass', 'replication_factor': 1};"
cqlsh "$CASSANDRA_HOST" -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS dis.fridge_table(uuid uuid primary key, timestamp timestamp, temperature double, condition text, attack text, label int);"
