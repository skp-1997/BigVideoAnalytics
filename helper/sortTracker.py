from sort.sort import Sort  # Import SORT library
import cv2
import numpy as np
from helper.detectObjects import detect_objects

# Initialize SORT tracker
tracker = Sort()

def detect_and_track_objects(frame_bytes):
    # try:
    np_arr = np.frombuffer(frame_bytes, np.uint8)
    frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    
    detected_objects = detect_objects(frame)

    '''
    if not detected_objects:
        return [{
            'track_id': None,
            'bounding_box': None}
        }]
    '''
    
    # Convert detected_objects to the format expected by SORT
    # detections = np.array([obj[1] + [obj[0]] for obj in detected_objects])
    # tracked_objects = tracker.update(detections)
    
    # Convert tracked_objects to unique IDs
    # tracking_data = []
    # for obj in tracked_objects:
    for obj in detected_objects:
        # x, y, w, h, track_id = obj
        # tracking_data.append({
        #     'track_id': int(track_id),
        #     'bounding_box': {'x': float(x), 'y': float(y), 'w': float(w), 'h': float(h)}
        # })
        '''
        print(f'[INFO] obj : {obj}')
        box = obj['bounding_box']
        class_label = obj['class']
        start_point = (int(box['x']), int(box['y']))
        end_point = (int(box['x'] + box['width']), int(box['y'] + box['height']))
        color = (0, 255, 0)  # Green color for bounding boxes
        thickness = 2
        '''
        print(f'[INFO] obj : {obj}')
        class_label = obj[0]
        [x, y, w, h] = obj[1]
        start_point = (x, y)
        end_point = (int(x+w), int(y+h))
        color = (0, 255, 0)  # Green color for bounding boxes
        thickness = 2


        # Draw the rectangle
        frame = cv2.rectangle(frame, start_point, end_point, color, thickness)

        # Put the label text
        label_position = (start_point[0], start_point[1] - 10)
        frame = cv2.putText(frame, class_label, label_position, cv2.FONT_HERSHEY_SIMPLEX, 0.9, color, 2)

    # Encode the image back to bytes
    _, buffer = cv2.imencode('.jpg', frame)
    return buffer.tobytes()
    # except Exception as e:
    #     print(f"Error processing frame: {e}")
    #     return 
    # return tracking_data
