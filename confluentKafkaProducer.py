import cv2
from confluent_kafka import Producer
from confluent_kafka import KafkaError
from pymongo import MongoClient
import concurrent.futures
import pandas as pd


# MongoDB connection setup
mongo_client = MongoClient('mongodb://127.0.0.1:27017')
db = mongo_client['bigvideo_analytics']
camera_collection = db['cameraDetails']


# Kafka configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9093',
    'security.protocol': 'PLAINTEXT'
}

# Initialize Kafka producer
producer = Producer(producer_conf)

# Define a delivery report callback to handle successful and failed deliveries
def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Worker function for processing each video stream
def process_video(camera_name, camera_path):
    topic_name = f"{camera_name}"# Unique topic name for each camera# Open the video file
    cap = cv2.VideoCapture(camera_path)

    if not cap.isOpened():
        print(f"Error opening video file: {camera_path}")
        return 
    
    while True:
        ret, frame = cap.read()
        if not ret:
            break # End of video# Encode the frame as JPEG

        _, buffer = cv2.imencode('.jpg', frame)
        frame_bytes = buffer.tobytes()

        # Send the frame to the Kafka topic
        try:
            producer.produce(topic=topic_name, value=frame_bytes, callback=delivery_report)
            producer.poll(0)  # Trigger the delivery report callback
        except KafkaError as e:
            print(f"Failed to send frame to Kafka: {e}")

    cap.release()  # Release the video capture object# Fetch camera URLs from MongoDB and process each video stream concurrently



def main():
    # List to hold camera information
    
    # Uses MongoDB to read camera details
    camera_info = []
    try:
        cameras = camera_collection.find()
        for camera in cameras:
            camera_name = camera.get('cameraName')
            camera_path = camera.get('cameraPath')

            if camera_name and camera_path:
                camera_info.append((camera_name, camera_path))

    except Exception as e:
        print(f"Failed to retrieve data from MongoDB: {e}")
    '''
    # Read camera details from CSV file using pandas
    camera_df = pd.read_csv("camera_details.csv")
    # print(camera_df)
    # Convert DataFrame to list of tuples
    camera_info_list = list(zip(camera_df['cameraName'], camera_df['cameraPath']))
    '''

    # Use ThreadPoolExecutor to process video streams concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(camera_info)) as executor:
        futures = [executor.submit(process_video, camera_name, camera_path) for camera_name, camera_path in camera_info]
        
        # Wait for all futures to complete
        concurrent.futures.wait(futures)

    # Block until all messages are sent
    producer.flush()

    # Close MongoDB connection
    # mongo_client.close()

if __name__ == '__main__':
    main()
