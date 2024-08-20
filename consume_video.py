from kafka import KafkaConsumer
import os
import uuid

# Kafka consumer 설정
consumer = KafkaConsumer(
    'TF-CAM-TEST',
    bootstrap_servers="piai_kafka.aiot.town:9092",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='video-consumer-group'
)

# 저장할 파일 경로 생성
output_dir = 'received_videos'
os.makedirs(output_dir, exist_ok=True)

# 메시지 수신 및 파일로 저장
for message in consumer:
    # 고유한 파일 이름 생성
    output_file = os.path.join(output_dir, f"{str(uuid.uuid4())}.mp4")
    
    # 수신된 데이터를 파일로 저장
    with open(output_file, 'wb') as video_file:
        video_file.write(message.value)
    
    print(f"Received and saved video to {output_file}")
