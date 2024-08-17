# Architecture of the project


![BigDataVideo drawio](https://github.com/user-attachments/assets/52548c39-2f1e-4e20-82b2-13d24758cb2c)

The producer reads frames from each video file / Live streama and publishes on the kafka server with topic name corresponding to video file name. The kafka store frames to corresponding topics.
The spark consumer consumes the frame and apply user defined fucntion (UDF) in this case face detector, for each frame and pushes to second and last kafka server. Finally the consumer at kafka end, writes the frames
acoording to topic name and save the processed videos to output folder.

# Installation

Use docker-compose file to spin kafka containers for two servers with listening port 9093, 9095, and zookeeper listening at 2181

```
docker-compose up -d
```

You can also install manually and run the kafka using kafka_start.sh, just make sure to create two different server.properties files in conf in kafka and 
change the broker id and listening port

Download and Install the spark by following instructions here. https://spark.apache.org/downloads.html
You can use docker file provided also for installing spark

Create Conda environment and install libraries mentioned in requirements.txt file

```
pip install -r requirements.txt
```
# Running the program

To start the producer, run

```
python confluentKafkaProducer
```

To start the spark consumer, run

Source the bash file
```
source ~/.bash_profile
```
Run the Spark
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 sparkConsumer.py
```

To start the kafka consumer to write the output video, run

```
python kafkaConsumer.py
```


# Useful Tips

To make spark access the conda environment libraries, use this on command line

```
export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)
```

To list the runnig kafka topics, use this

```
bin/kafka-topics.sh --list --bootstrap-server localhost:PORT
```

To list the delete kafka topics, use this

```
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic your_topic_name
```

