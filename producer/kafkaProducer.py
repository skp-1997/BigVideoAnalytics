import cv2
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pymongo import MongoClient
import concurrent.futures

# MongoDB connection setup
mongo_client = MongoClient('mongodb://127.0.0.1:27017')
db = mongo_client['bigvideo_analytics']
camera_collection = db['cameraDetails']

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    security_protocol='PLAINTEXT',
    value_serializer=lambda v: v  # Directly serialize messages to bytes
)

# Define a callback function for successful message delivery
def on_send_success(record_metadata):
    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

# Define a callback function for message delivery errors
def on_send_error(excp):
    print(f"Failed to send message: {excp}")

# Worker function for processing each video stream
def process_video(camera_name, camera_path):
    topic_name = f"camera_{camera_name}"  # Unique topic name for each camera

    # Open the video file
    cap = cv2.VideoCapture(camera_path)

    if not cap.isOpened():
        print(f"Error opening video file: {camera_path}")
        return

    while True:
        ret, frame = cap.read()
        if not ret:
            break  # End of video

        # Encode the frame as JPEG
        _, buffer = cv2.imencode('.jpg', frame)
        frame_bytes = buffer.tobytes()

        # Send the frame to the Kafka topic
        try:
            future = producer.send(topic_name, value=frame_bytes)
            future.add_callback(on_send_success).add_errback(on_send_error)
        except KafkaError as e:
            print(f"Failed to send frame to Kafka: {e}")

    cap.release()  # Release the video capture object

# Fetch camera URLs from MongoDB and process each video stream concurrently
def main():
    # List to hold camera information
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

    # Use ThreadPoolExecutor to process video streams concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(camera_info)) as executor:
        futures = [executor.submit(process_video, camera_name, camera_path) for camera_name, camera_path in camera_info]
        
        # Wait for all futures to complete
        concurrent.futures.wait(futures)

    # Block until all messages are sent
    producer.flush()

    # Close the producer
    producer.close()

    # Close MongoDB connection
    mongo_client.close()

if __name__ == '__main__':
    main()
