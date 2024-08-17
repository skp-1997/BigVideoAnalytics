import cv2
import numpy as np
import pymongo
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import concurrent.futures
from helper import sparkConsumer

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb://127.0.0.1:27017')
db = mongo_client['bigvideo_analytics']
camera_collection = db['cameraDetails']

# Function to initialize Kafka consumer and video writer for a topic
def process_topic(topic_name, output_file, frame_size, fps):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        group_id=f'{topic_name}_consumer_group',
        value_deserializer=lambda m: m  # Directly handle bytes
    )

    # Create a VideoWriter object to save the video
    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    video_writer = cv2.VideoWriter(output_file, fourcc, fps, frame_size)

    print(f"Starting to consume frames from topic {topic_name} and save to {output_file}...")

    try:
        for message in consumer:
            frame_bytes = message.value

            # Decode the frame bytes into an image
            np_arr = np.frombuffer(frame_bytes, np.uint8)
            frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

            if frame is not None:
                # Write the frame to the video file
                video_writer.write(frame)
            else:
                print(f"Failed to decode frame from topic {topic_name}")
                
    except KafkaError as e:
        print(f"Kafka error for topic {topic_name}: {e}")

    finally:
        # Release the VideoWriter and close resources
        video_writer.release()
        consumer.close()
        print(f"Finished saving video for topic {topic_name}")

def main():
    # Query MongoDB to find active camera topics
    camera_info = []
    try:
        cameras = camera_collection.find({'is_Activated': True})
        for camera in cameras:
            camera_name = camera.get('cameraName')
            camera_path = camera.get('cameraPath')  # You might not need this path for the consumer
            topic_name = f"camera_{camera_name}"  # Construct the topic name
            output_file = f"{camera_name}_output.avi"  # Output file name based on camera name

            if camera_name:
                camera_info.append((topic_name, output_file))

    except Exception as e:
        print(f"Failed to retrieve data from MongoDB: {e}")

    # Define frame size and FPS (must match the producer settings)
    frame_size = (640, 480)  # Adjust according to the video resolution
    fps = 30  # Frames per second (adjust as needed)

    # Use ThreadPoolExecutor to handle multiple topics concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(camera_info)) as executor:
        futures = []
        for topic_name, output_file in camera_info:
            # Submit a thread for each topic
            future = executor.submit(process_topic, topic_name, output_file, frame_size, fps)
            futures.append(future)
        
        # Wait for all futures to complete
        concurrent.futures.wait(futures)

    # Close MongoDB connection
    mongo_client.close()

if __name__ == '__main__':
    main()
