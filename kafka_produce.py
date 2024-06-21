import cv2
from kafka import KafkaProducer
import time

def produce_camera_stream(server_ip, server_port, topic_name):
    producer=KafkaProducer(bootstrap_servers=f"{server_ip}:{server_port}")

    cap=cv2.VideoCapture(0)

    while cap.isOpened():
        ret, frame=cap.read()
        if not ret:
            print("Failed to grab frame")
            break

        ret, buffer=cv2.imencode(".jpg", frame)
        if not ret:
            print("Failed to encode frame")
            break

        frame_bytes=buffer.tobytes()
        producer.send(topic_name, frame_bytes)

        time.sleep(0.1)

    cap.release()
    producer.close()

if __name__=="__main__":
    produce_camera_stream("piai_kafka.aiot.town", "9092", "TF-CAM-DOOR1")


