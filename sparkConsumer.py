import cv2
import numpy as np
import pymongo
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BinaryType
from pyspark.sql import SparkSession
from confluent_kafka import Producer
from helper.faceDetector import detect_faces
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaObjectTracking") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()


# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb://127.0.0.1:27017')
db = mongo_client['bigvideo_analytics']
camera_collection = db['cameraDetails']


def get_active_camera_topics():
    active_cameras = camera_collection.find({'is_Activated': True})
    topics = [f"{camera['cameraName']}" for camera in active_cameras if camera.get('cameraName')]
    return topics

# Get the list of active camera topics
active_camera_topics = get_active_camera_topics()
mongo_client.close()

'''
# Read camera details from CSV file using pandas
camera_df = pd.read_csv("camera_details.csv")
# Convert DataFrame to list of tuples
active_camera_topics = camera_df['cameraName'].tolist()
'''


# Kafka parameters
kafka_bootstrap_servers = 'localhost:9093'
new_kafka_bootstrap_servers = 'localhost:9095'  # New Kafka server
topics = ','.join(active_camera_topics)  # Comma-separated list of topics
print(f'<<<<<<<<<<<<<<<<<<<<<<<<<<<[INFO] The topics are {topics}>>>>>>>>>>>>>>>>>>>>>>>>>>')

# print(f'<<<<<<<<[INFO] The topics are {topics}>>>>>>>>>')

# Define Kafka stream DataFrame
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topics) \
    .load()

# Prepare DataFrame for processing
processed_df_for_save = kafka_stream_df.select(
    col("value").alias("frame_bytes"),
    col("topic")
)

# Initialize Kafka producer
producer = Producer({'bootstrap.servers': new_kafka_bootstrap_servers})

'''
# Load dlib face detector
detector = dlib.get_frontal_face_detector()

def detect_faces(frame_bytes):
    # Convert binary frame to image format
    nparr = np.frombuffer(frame_bytes, np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    
    if frame is None:
        return frame_bytes  # Return the original frame if there's an issue decoding

    # Convert to grayscale for face detection
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    
    # Detect faces
    faces = detector(gray)
    
    # Draw bounding boxes around faces
    for face in faces:
        x, y, w, h = (face.left(), face.top(), face.width(), face.height())
        cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 5)
        # cv2.imwrite("out.jpg", frame)
    
    # Encode frame back to bytes
    _, encoded_frame = cv2.imencode('.jpg', frame)
    return encoded_frame.tobytes()
'''

# Register UDF with Spark
detect_faces_udf = udf(detect_faces, BinaryType())

# Apply UDF to process frames
processed_df_with_faces = processed_df_for_save.withColumn(
    "frame_bytes_with_faces",
    detect_faces_udf(col("frame_bytes"))
)
# print(f'<<<<<<<<<<<<<[INFO] Applied UDF>>>>>>>>>>>>>>>>>>>')

def push_frames_to_kafka(batch_df, epoch_id):
    # print(f'[INFO] Pushing frames to kafka...............................')
    for row in batch_df.collect():
        frame_bytes = row['frame_bytes_with_faces']
        # Ensure frame_bytes is of type bytes (read-only)
        if isinstance(frame_bytes, bytearray):
            frame_bytes = bytes(frame_bytes)

        '''
        nparr_out = np.frombuffer(frame_bytes, np.uint8)
        frame_out = cv2.imdecode(nparr_out, cv2.IMREAD_COLOR)
        cv2.imwrite("out_frame.jpg", frame_out)
        '''

        topic = row['topic']
        
        # Create corresponding output topic
        output_topic = f"output_{topic}"
        # print(f'<<<<<<<<<<<[INFO] The output topic is {output_topic}.....')
        
        try:
            producer.produce(topic=output_topic, value=frame_bytes)
            producer.flush()
            print(f"Frame with faces sent to topic: {output_topic}")
        except Exception as e:
            print(f"Error sending frame to topic: {output_topic}, Error: {e}")

# Write the frames to new Kafka topics
query = processed_df_with_faces.writeStream \
    .foreachBatch(push_frames_to_kafka) \
    .outputMode("update") \
    .start()

try:
    query.awaitTermination()
finally:
    producer.flush()
