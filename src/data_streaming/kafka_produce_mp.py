import cv2
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import time
import numpy as np
import multiprocessing as mp

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

def produce_frame(server_ip, server_port, topic_name, frame_queue):
    producer = KafkaProducer(bootstrap_servers=f"{server_ip}:{server_port}")
    last_offset = get_last_offset(server_ip, server_port, topic_name)
    while True:
        frame = frame_queue.get()
        if frame is None:
            break
        _, buffer = cv2.imencode(".jpg", frame)
        frame_bytes = buffer.tobytes()
        last_offset += 1
        message_key = str(last_offset).encode('utf-8')
        producer.send(topic_name, key=message_key, value=frame_bytes)
    producer.close()

def capture_frames(frame_queue):
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
        if ret:
            frame_queue.put(frame)
            frame_count += 1
            if time.time() - start_time >= 1:
                print(f"fps: {frame_count}")
                frame_count = 0
                start_time = time.time()
    cap.release()
    frame_queue.put(None)  # signal producer to stop

if __name__ == "__main__":
    frame_queue = mp.Queue(maxsize=10)
    server_ip = "piai_kafka.aiot.town"
    server_port = "9092"
    topic_name = "TF-CAM-TEST"
    
    producer_process = mp.Process(target=produce_frame, args=(server_ip, server_port, topic_name, frame_queue))
    producer_process.start()
    
    capture_frames(frame_queue)
    
    producer_process.join()
