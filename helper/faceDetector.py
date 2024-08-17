import cv2 
import dlib
import numpy as np

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
        cv2.rectangle(frame, (x, y), (x + w, y + h), (255, 0, 0), 2)
    
    # Encode frame back to bytes
    _, encoded_frame = cv2.imencode('.jpg', frame)
    return encoded_frame.tobytes()
    # return frame

'''
# Read the input image file
img_path = '/Users/surajpatil/Documents/GitHub/BigVideoAnalytics/output.jpg'
img = cv2.imread(img_path)

# Encode the frame as JPEG
_, buffer = cv2.imencode('.jpg', img)
frame_bytes = buffer.tobytes()

# Detect faces and get processed frame bytes
out = detect_faces(frame_bytes)

# Decode the output frame bytes and save to file
nparr_out = np.frombuffer(out, np.uint8)
frame_out = cv2.imdecode(nparr_out, cv2.IMREAD_COLOR)
cv2.imwrite("out.jpg", frame_out)
'''