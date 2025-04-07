import cv2
# import torch
# from torchvision import transforms
import datetime
import numpy as np
import ultralytics
ultralytics.checks()
from ultralytics import YOLO

import pickle
# import numpy as np
from scipy.stats import multivariate_normal

from json import dumps
import json
import sys, types
import numpy
# from kafka_consume import consumer_temp_video
from kafka import KafkaConsumer, TopicPartition, KafkaProducer

def detect_fire_temp_video(model, server_ip, server_port, topic_name, group_id, json_producer,
                           display=False):
    
    consumer = KafkaConsumer(
        # f"{topic_name}",
        bootstrap_servers=f"{server_ip}:{server_port}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=f"{group_id}",
        api_version=(2, 6, 0)
    )
    consumer.subscribe(topic_name)
    
    kafka_dir = {"box_offset": None,
                 "frame_offset": None,
                 "temp_offset": None,
                "class": None,
                "confidence": None,
                "camera_fire": None,
                "temperature": None,
                "temp_fire": None,
                "detect_fire": None,
                }
    
    while(1):
        # poll messages each certain ms
        # raw_messages = consumer.poll(
        #     timeout_ms=100, max_records=200
        # )
        raw_messages = consumer.poll(1.0)
        # for each messages batch
        for topic_partition, messages in raw_messages.items():
            # if message topic is k_connectin_status
            if topic_partition.topic == "DJ-CAM-SM1":
              # process_k_connectin_status(messages)
                for message in messages:
                    frame_bytes = message.value
                    frame_key = message.offset
                    np_arr = np.frombuffer(frame_bytes, dtype=np.uint8)
                    frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

                    results = model(frame, conf=0.2, classes=[2], device=0, verbose=False)

                    camera_fire = 0
                    for cls in results[0].boxes.cls:
                        if cls != 1:
                            camera_fire = 1
                            break
                    kafka_dir["frame_offset"] = frame_key
                    kafka_dir["box_offset"] = results[0].boxes.xyxyn.tolist()
                    kafka_dir["class"] = results[0].boxes.cls.tolist()
                    kafka_dir["confidence"] = results[0].boxes.conf.tolist()
                    kafka_dir["camera_fire"] = camera_fire
                    
                    annotated_frame = results[0].plot()

                    if display == True:
                        # Display the annotated frame
                        cv2.imshow("YOLOv8 Inference", annotated_frame)
                        key = cv2.waitKey(10)
                        
                        if key == ord('q'):
                            break
                        else:
                            break
                    
            elif topic_partition.topic == "DJ-SENSOR-DATA-SM1":
                for message in messages:
                    frame_bytes = message.value.decode('utf-8')
                    key = message.offset
                    temp = json.loads(frame_bytes)["ambient_temperature"]
                    
                    temp_fire = calculate_probability_density(temp, means, covariances, weights)
                    kafka_dir["temp_offset"] = key
                    kafka_dir["temp_fire"] = temp_fire
                    # if frame_bytes is not None:
                    #     print(f'temp_key : {key}')
                    #     print(f'temperature : {temp}')
                    #     # return temp
                    # else:
                    #     print("Failed to load temp")
            
            if kafka_dir["camera_fire"] == 1 and kafka_dir["temp_fire"] == 1:
                kafka_dir["detect_fire"] = 1
            else:
                kafka_dir["detect_fire"] = 0
                
            print(f'Sending kafka_dir : {kafka_dir}')
            
            json_producer.send("DJ-FIRE-INFERENCE-SM1", value=kafka_dir)
            
            

    if display == True:
       cv2.destroyAllWindows()
    


# python version에 따라 해당 코드가 필요할 수 있음 (3.11 이상?)
m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
setattr(m, 'range', range)
sys.modules['kafka.vendor.six.moves'] = m
bootstrap_servers=['141.223.107.155:9092',]
# json 예시

json_producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers, # 전달하고자 하는 카프카 브로커의 주소 리스트
    value_serializer=lambda x:dumps(x).encode('utf-8') # 메시지의 값 직렬화
)

# Temperature based detection
with open('/home/aiot/rpi4_kafka_test/checkpoints/gmm_params.pkl', 'rb') as file:
    params = pickle.load(file)
means = params['means']
covariances = params['covariances']
weights = params['weights']


# 특정 데이터 포인트에 대한 확률 밀도 계산 함수
def calculate_probability_density(X, means, covariances, weights, thresh=0.0007):
    # 전체 확률 밀도를 0으로 초기화
    total_density = 0
    # 각 GMM 컴포넌트를 순회하면서 확률 밀도를 계산
    for mean, cov, weight in zip(means, covariances, weights):
        # 현재 컴포넌트의 확률 밀도 계산
        rv = multivariate_normal(mean, cov)
        component_density = rv.pdf(X)
        # 가중치를 적용하여 전체 확률 밀도에 더함
        total_density += weight * component_density
        if total_density < thresh and X > 50:
            temp_fire = 1
        else:
            temp_fire = 0
            
    return temp_fire
#%


model = YOLO('/home/aiot/rpi4_kafka_test/checkpoints/last.pt')

# results = model(image_path)

detect_fire_temp_video(model, "piai_kafka.aiot.town", "9092", ["DJ-CAM-SM1", "DJ-SENSOR-DATA-SM1"], "webcam-group", json_producer)

# cap = cv2.VideoCapture(0)
# frame_idx = 0
# temp = [65]
# # Loop through the video frames
# while cap.isOpened():
#     frame_idx += 1
#     # Read a frame from the video
#     success, frame = cap.read()

#     if success:
#         # Run YOLOv8 inference on the frame
#         results = model(frame, conf=0.2, classes=[2])

#         camera_fire = 0
#         for cls in results[0].boxes.cls:
#             if cls != 1:
#                 camera_fire = 1
#                 break

#         temp_fire = calculate_probability_density(temp, means, covariances, weights)
        
#         kafka_dir = {"offset": results[0].boxes.xyxyn.tolist(),
#                      "class": results[0].boxes.cls.tolist(),
#                      "confidence": results[0].boxes.conf.tolist(),
#                      "camera_fire": camera_fire,
#                      "temperature": temp,
#                      "temp_fire": temp_fire,
#                      "detect_fire": camera_fire*temp_fire
#                      }
#         json_producer.send("DJ-FIRE-INFERENCE-SM1", value=kafka_dir)
        
#         # output_file = "kafka_send.json"
#         # with open(output_file, 'w') as f:
#         #     json.dump(kafka_dir, f, indent=4)
#         # # # Visualize the results on the frame

#         annotated_frame = results[0].plot()

#         # Display the annotated frame
#         cv2.imshow("YOLOv8 Inference", annotated_frame)
#         key = cv2.waitKey(10)
        
#         if key == ord('q'):
#             break
#     else:
#         print(success)
#         break
    
# cv2.destroyAllWindows()
