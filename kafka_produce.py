import cv2
from kafka import KafkaProducer, KafkaAdminClient, TopicPartition
import time

def get_last_offset(admin_client, topic_name):
    metadata = admin_client.describe_topics([topic_name])
    partitions = metadata[0].partitions
    last_offset = 0

    for partition in partitions:
        partition_id = partition.partition
        topic_partition = TopicPartition(topic_name, partition_id)
        offset_info = admin_client.list_offsets({topic_partition: -1})
        partition_offset = offset_info[topic_partition].offset
        last_offset = max(last_offset, partition_offset)

    return last_offset

def produce_camera_stream(server_ip, server_port, topic_name):
    producer = KafkaProducer(bootstrap_servers=f"{server_ip}:{server_port}")
    admin_client = KafkaAdminClient(bootstrap_servers=f"{server_ip}:{server_port}")

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
        last_offset = get_last_offset(admin_client, topic_name)
        message_key = str(last_offset).encode('utf-8')

        producer.send(topic_name, key=message_key, value=frame_bytes)

        time.sleep(0.1)

    cap.release()
    producer.close()
    admin_client.close()

if __name__ == "__main__":
    produce_camera_stream("piai_kafka.aiot.town", "9092", "TF-CAM-DOOR1")
