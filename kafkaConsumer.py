import cv2
import numpy as np
import pymongo
from confluent_kafka import Consumer, KafkaException
from confluent_kafka import KafkaError
import os
import pandas as pd


# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb://127.0.0.1:27017')
db = mongo_client['bigvideo_analytics']
camera_collection = db['cameraDetails']

def get_active_camera_topics():
    active_cameras = camera_collection.find({'is_Activated': True})
    topics = [f"output_{camera['cameraName']}" for camera in active_cameras if camera.get('cameraName')]
    return topics

# Get the list of active camera topics
active_camera_topics = get_active_camera_topics()
mongo_client.close()


'''
# Read camera details from CSV file using pandas
camera_df = pd.read_csv("camera_details.csv")
# Convert DataFrame to list of tuples
active_camera_topics = []
active_camera = camera_df['cameraName'].tolist()
for camera_name in active_camera:
    active_camera_topics.append(f'output_{camera_name}')
print(f'[INFO] Active cameras are : {active_camera_topics}')
'''

# Kafka parameters
kafka_bootstrap_servers = 'localhost:9095'
group_id = 'single_consumer_group'  # Replace with your Kafka consumer group ID

# Create a directory to save the videos
output_dir = 'output_videos'
os.makedirs(output_dir, exist_ok=True)

# Initialize Kafka consumer
consumer = Consumer({
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
})

# Subscribe to Kafka topics
consumer.subscribe(active_camera_topics)

# Dictionary to keep track of video writers
video_writers = {}

def save_frame_to_file(topic, frame_bytes):
    global video_writers

    # Convert the binary frame back to image format
    nparr = np.frombuffer(frame_bytes, np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    if frame is None:
        print(f"Error decoding frame for topic: {topic}")
        return

    # Initialize the video writer if not already done
    if topic not in video_writers:
        height, width, _ = frame.shape
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        file_name = os.path.join(output_dir, f"{topic}.mp4")
        video_writers[topic] = cv2.VideoWriter(file_name, fourcc, 30.0, (width, height))

    # Write the current frame to the video file
    try:
        video_writers[topic].write(frame)
        # print(f"Frame written to {file_name}")
        # cv2.imwrite("output.jpg", frame)
    except Exception as e:
        print(f"Error writing frame to {file_name}: {e}")

def main():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Adjust the timeout as needed

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            topic = msg.topic()
            # print(f'[INFO] The topic is {topic}........')
            frame_bytes = msg.value()

            save_frame_to_file(topic, frame_bytes)

    except KeyboardInterrupt:
        pass
    finally:
        # Release all video writers
        for writer in video_writers.values():
            writer.release()
        consumer.close()
        print(f'[INFO] Gracefully exited!')

if __name__ == '__main__':
    main()
