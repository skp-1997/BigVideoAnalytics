import cv2
import numpy as np
import sys

# Load pre-trained object detection model (e.g., YOLO)
net = cv2.dnn.readNet('/Users/surajpatil/Documents/GitHub/BigVideoAnalytics/models/yolov3.weights', '/Users/surajpatil/Documents/GitHub/BigVideoAnalytics/models/yolov3.cfg')
layer_names = net.getLayerNames()
output_layers = [layer_names[i - 1] for i in net.getUnconnectedOutLayers()]
'''
modelConfiguration = '/Users/surajpatil/Documents/GitHub/BigVideoAnalytics/models/yolov3.weights'
modelWeights = '/Users/surajpatil/Documents/GitHub/BigVideoAnalytics/models/yolov3.cfg'
net = cv2.dnn.readNetFromDarknet(modelConfiguration, modelWeights)
net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
'''

def detect_objects(frame):
    height, width, channels = frame.shape
    blob = cv2.dnn.blobFromImage(frame, 0.00392, (416, 416), (0, 0, 0), True, crop=False)
    net.setInput(blob)
    outs = net.forward(output_layers)

    class_ids = []
    confidences = []
    boxes = []

    for out in outs:
        for detection in out:
            # Objectness score and class probabilities
            scores = detection[5:]
            # Get class ID and confidence
            class_id = np.argmax(scores)
            confidence = scores[class_id]
            
            if confidence > 0.5:
                # Process each detection
                # Extract bounding box coordinates, objectness score, and class probabilities
                center_x = int(detection[0] * width)
                center_y = int(detection[1] * height)
                w = int(detection[2] * width)
                h = int(detection[3] * height)
                x = int(center_x - w / 2)
                y = int(center_y - h / 2)
                boxes.append([x, y, w, h])
                confidences.append(float(confidence))
                class_ids.append(class_id)

    # Apply non-max suppression to remove duplicate boxes
    indices = cv2.dnn.NMSBoxes(boxes, confidences, 0.5, 0.4)
    # print(indices)
    if len(indices) == 0:
        print(f'[INFO] No object detected...')
        return []
    # sys.exit()
    detected_objects = [(class_ids[i], boxes[i]) for i in indices.flatten()]
    
    return detected_objects

# # Load the image
# image = cv2.imread('/Users/surajpatil/Documents/GitHub/BigVideoAnalytics/input/car.jpeg')

# Detect objects in the image
# print(detect_objects(image))
