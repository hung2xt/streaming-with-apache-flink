#!/bin/bash

#This optional if you don't use Docker

# Define the path to the Kafka directory
KAFKA_HOME=/Users/hungnp14/bigdata/kafka_2.12-2.6.3

echo "Stopping Kafka..."
$KAFKA_HOME/bin/kafka-server-stop.sh

# Wait for Kafka to stop
sleep 10

echo "Stopping Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-stop.sh

echo "Kafka and Zookeeper have been stopped."
