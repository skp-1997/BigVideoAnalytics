# Architecture of the project


![BigDataVideo drawio](https://github.com/user-attachments/assets/52548c39-2f1e-4e20-82b2-13d24758cb2c)

The architecture consists of the following components:

- Producer: Reads frames from video files or live streams and publishes them to a Kafka server. Each frame is sent to a topic corresponding to the video file name.
- Kafka Server: Stores frames in their respective topics.
- Spark Consumer: Consumes frames from Kafka, applies a user-defined function (UDF), such as a face detector, and pushes processed frames to a second Kafka server.
- Final Kafka Consumer: Writes frames according to the topic name and saves the processed videos to the output folder.

# Installation

## Using Docker Compose


Spin up Kafka containers for two servers (listening on ports 9093 and 9095) and Zookeeper (listening on port 2181) using Docker Compose:

```
docker-compose up -d
```
## Manual Installation

1. Kafka:

    - Run Kafka using kafka_start.sh.
    - Ensure you create two different server.properties files in the conf directory and adjust the broker ID and listening port.
      
2. Spark:

    - Download and install Spark from Apache Spark Downloads.
    - Alternatively, use the provided Dockerfile for Spark installation.

3. Python Libraries:

    - Create a Conda environment and install the required libraries from requirements.txt:
      
      ```
      pip install -r requirements.txt
      ```
# Running the program

1. Start the Producer:
      ```
      python confluentKafkaProducer
      ```
2. Start the Spark Consumer:

  - Source the bash profile:
      ```
      source ~/.bash_profile
      ```
  - Run Spark with the following command:
    
      ```
      spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 sparkConsumer.py
      ```

3. Start the Kafka Consumer:

      ```
      python kafkaConsumer.py
      ```


# Useful Tips

- To ensure Spark can access Conda environment libraries, set these environment variables:

      ```
      export PYSPARK_PYTHON=$(which python)
      export PYSPARK_DRIVER_PYTHON=$(which python)
      ```
- To list running Kafka topics:

      ```
      bin/kafka-topics.sh --list --bootstrap-server localhost:PORT
      ```

- To delete a Kafka topic:

      ```
      kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic your_topic_name
      ```

