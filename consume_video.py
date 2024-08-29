from kafka import KafkaConsumer, TopicPartition
import os
import uuid

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

# Kafka consumer 설정
consumer = KafkaConsumer(
    'TF-CAM-TEST',
    bootstrap_servers="piai_kafka.aiot.town:9092",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=str(uuid.uuid4())
)
print(get_last_offset("piai_kafka.aiot.town", "9092", "TF-CAM-TEST"))

output_dir = 'received_videos'
os.makedirs(output_dir, exist_ok=True)

print("Starting to consume messages...")
for message in consumer:
    print("Message received!")
    output_file = os.path.join(output_dir, f"{str(uuid.uuid4())}.mp4")
    
    with open(output_file, 'wb') as video_file:
        video_file.write(message.value)
    
    print(f"Received and saved video to {output_file}")
