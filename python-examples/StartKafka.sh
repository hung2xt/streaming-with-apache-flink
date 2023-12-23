#!/bin/bash

# This optional if you don't use Docker

# Define the path to the Kafka directory
KAFKA_HOME=/Users/hungnp14/bigdata/kafka_2.12-2.6.3

echo "Starting Zookeeper..."
nohup $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > zookeeper.log 2>&1 &

# Wait for a few seconds to ensure Zookeeper starts before Kafka
sleep 5

echo "Starting Kafka..."
nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > kafka.log 2>&1 &

echo "Kafka and Zookeeper have been started."


