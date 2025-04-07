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
def produce_camera_stream(server_ip, server_port, topic_name, threshold):
    while True:
        producer = KafkaProducer(bootstrap_servers=f"{server_ip}:{server_port}")
        cap = cv2.VideoCapture(0)
        if not cap.isOpened():
            print("Failed to open camera")
            return
        cap.set(cv2.CAP_PROP_FPS, FPS)
        cur_fps=cap.get(cv2.CAP_PROP_FPS)
        print(f"cur_fps: {cur_fps}")
        start_time=time.time()
        start_offset=get_last_offset(server_ip, server_port, topic_name)
        msg_num=0
        while cap.isOpened():
            ret, frame = cap.read()
            if ret:
                ret, buffer = cv2.imencode(".jpg", frame)
                frame_bytes = buffer.tobytes()
                msg_num += 1
                message_key = str(start_offset+msg_num).encode('utf-8')
                producer.send(topic_name, key=message_key, value=frame_bytes)
            diff_msg_num=(time.time()-start_time)*cur_fps-msg_num
            print(f"ideal msg_num - real msg_num: {diff_msg_num}")
            if diff_msg_num > threshold:
                cap.release()
                producer.close()
                print(f"restart the VideoCapture and KafkaProducer")
                break

if __name__ == "__main__":
    produce_camera_stream("piai_kafka.aiot.town", "9092", "TF-CAM-TEST", 150)