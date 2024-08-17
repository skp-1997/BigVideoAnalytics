#!/bin/bash

# Use it to start kafka without docker 

# Start Zookeeper
echo "Starting Zookeeper..."
cd /Users/surajpatil/Documents/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties > zookeeper.log 2>&1 &
ZOOKEEPER_PID=$!
echo "Zookeeper started with PID $ZOOKEEPER_PID"

# Wait for Zookeeper to start up (optional, adjust time as needed)
sleep 10

# Start Kafka broker
echo "Starting Kafka Server..."
cd /Users/surajpatil/Documents/kafka
bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &
KAFKA_PID=$!
echo "Kafka Server started with PID $KAFKA_PID"

# Wait for Kafka to start up (optional, adjust time as needed)
wait $ZOOKEEPER_PID
wait $KAFKA_PID

