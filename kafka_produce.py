import cv2
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import time
import numpy as np
FPS = 30
MAX_FPS = 30
def get_last_offset(server_ip, server_port, topic_name):
    consumer = KafkaConsumer(bootstrap_servers=f"{server_ip}:{server_port}")
    partitions = consumer.partitions_for_topic(topic_name)
    last_offset = 0
    if partitions is not None:
        for partition in partitions:
            tp = TopicPartition(topic_name, partition)
            consumer.assign([tp])
            consumer.seek_to_end(tp)
            offset = consumer.position(tp)
            last_offset = max(last_offset, offset)
    consumer.close()
    return last_offset
def produce_camera_stream(server_ip, server_port, topic_name):
    producer = KafkaProducer(bootstrap_servers=f"{server_ip}:{server_port}")
    prev = 0
    last_offset = get_last_offset(server_ip, server_port, topic_name)
    cap = cv2.VideoCapture(0)
    cap.set(cv2.CAP_PROP_FPS, FPS)
    print('width: {}, height : {}'.format(cap.get(3), cap.get(4)))
    if not cap.isOpened():
        print("Failed to open camera")
        return
    frame_count = 0
    start_time = time.time()
    while cap.isOpened():
        ret, frame = cap.read()
        if True:
            prev = time.time()
            if prev - start_time >= 1:
                print(f"fps: {frame_count}")
                frame_count = 0
                start_time = prev
            ret, buffer = cv2.imencode(".jpg", frame)
            frame_bytes = buffer.tobytes()
            last_offset += 1
            message_key = str(last_offset).encode('utf-8')
            producer.send(topic_name, key=message_key, value=frame_bytes)
            frame_count += 1
    cap.release()
    producer.close()
if __name__ == "__main__":
    produce_camera_stream("piai_kafka.aiot.town", "9092", "TF-CAM-TEST")