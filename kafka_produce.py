import cv2
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import time

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

    cap = cv2.VideoCapture(0)

    if not cap.isOpened():
        print("Failed to open camera")
        return

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            print("Failed to grab frame")
            break

        ret, buffer = cv2.imencode(".jpg", frame)
        if not ret:
            print("Failed to encode frame")
            break

        frame_bytes = buffer.tobytes()

        # Get the last offset
        last_offset = get_last_offset(server_ip, server_port, topic_name)
        message_key = str(last_offset).encode('utf-8')

        producer.send(topic_name, key=message_key, value=frame_bytes)

        time.sleep(0.1)

    cap.release()
    producer.close()

if __name__ == "__main__":
    produce_camera_stream("piai_kafka.aiot.town", "9092", "TF-CAM-DOOR1")
