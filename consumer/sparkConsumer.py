from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BinaryType, StringType
import cv2
import numpy as np
import pymongo
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaFrameProcessing") \
    .getOrCreate()

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb://127.0.0.1:27017')
db = mongo_client['bigvideo_analytics']
camera_collection = db['cameraDetails']

def get_active_camera_topics():
    active_cameras = camera_collection.find({'is_Activated': True})
    topics = [f"camera_{camera['cameraName']}" for camera in active_cameras if camera.get('cameraName')]
    return topics

# Get the list of active camera topics
active_camera_topics = get_active_camera_topics()
mongo_client.close()

# Kafka parameters
kafka_bootstrap_servers = 'localhost:9092'
topics = ','.join(active_camera_topics)  # Comma-separated list of topics

# Define a DataFrame reader for Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topics) \
    .load()

# Add the topic name to the DataFrame
def extract_topic_name(topic_partition):
    return topic_partition.split("_")[1]

# UDF to convert Kafka binary data to an image (frame)
def decode_frame(frame_bytes):
    np_arr = np.frombuffer(frame_bytes, np.uint8)
    frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    return frame

def process_frame_udf(frame_bytes):
    frame = decode_frame(frame_bytes)
    if frame is not None:
        # Convert to a format that Spark can work with
        _, buffer = cv2.imencode('.jpg', frame)
        return buffer.tobytes()
    else:
        return None

# Register the UDF
process_frame = udf(process_frame_udf, BinaryType())

# Add topic name as a column
kafka_stream_df = kafka_stream_df.withColumn('topic_name', col('topic'))

# Apply UDF to process frames
processed_df = kafka_stream_df.select(
    col("value").alias("frame_bytes"),
    col("topic_name")
).select(
    process_frame(col("frame_bytes")).alias("processed_frame"),
    col("topic_name")
)

# Define the output directory based on the topic
def write_to_topic(topic_name, frame_data):
    output_dir = f"/path/to/output/{topic_name}"
    os.makedirs(output_dir, exist_ok=True)
    frame_path = os.path.join(output_dir, f"{topic_name}.jpg")

    # Save frame data to a file
    with open(frame_path, 'wb') as f:
        f.write(frame_data)

# Define a custom sink for processing
def custom_sink(df, epoch_id):
    for row in df.collect():
        topic_name = row['topic_name']
        frame_data = row['processed_frame']
        write_to_topic(topic_name, frame_data)

query = processed_df.writeStream \
    .foreachBatch(custom_sink) \
    .start()

query.awaitTermination()
