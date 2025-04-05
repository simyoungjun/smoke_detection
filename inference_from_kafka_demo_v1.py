import cv2
# import torch
import torchvision
# from torchvision import transforms
import datetime
import numpy as np
import ultralytics
ultralytics.checks()
from ultralytics import YOLO
import time
import pickle
# import numpy as np
from scipy.stats import multivariate_normal

from json import dumps
import json
import sys, types
import numpy
# from kafka_consume import consumer_temp_video
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
import base64
from collections import deque
import matplotlib.pyplot as plt

import torch

print("CUDA available:", torch.cuda.is_available())
print("CUDA version:", torch.version.cuda)
print("Device name:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "No GPU detected")


def detect_fire_temp_video(model, json_producer, display=False):
    kafka_dir = {
                "temperature": None,
                "detect_fire": None,
                "camera_confidence": None,
                "risk": None,
                "fire_image": None
                
                }
    #rtsp_url = 0
    #rasp_url = "rtsp://141.223.107.155:8554/live"
    rtsp_url = "rtsp://localhost:8554/live"
    # rtsp_url = "rtsp://100.78.6.108:8554/stream1"
    #rtsp_url = "rtsp://jinyou91@postech.ac.kr:Monetghgh1212@100.71.100.88:10000/stream2"
    cap = cv2.VideoCapture(rtsp_url)

    if not cap.isOpened():
        print("Error: Unable to connect to RTSP stream")
        exit()
       
    # Temperature variables
    temp = 55  # Initial temperature
    camera_fire = False
    camera_fire_first_time = 0  # Time when smoke was first detected 
    previous_camera_fire = False
    last_detection_time = time.time()
    risk = 0
    
    detect_queue = deque()
    conf_queue = deque()
    
    recap = 0
    detect_fire = 0
    # is_show = False
    while True:
        previous_camera_fire = camera_fire
        
        # 현재 시간 확인
        current_time = time.time()
        # 1초 간격으로 처리
        if current_time - last_detection_time >= 1.0:
            current_time = time.time()
            
            ret, frame = cap.read()
            while not ret:
                cap.release()
                time.sleep(2)
                cap = cv2.VideoCapture(rtsp_url)
                ret, frame = cap.read()
                recap += 1
                
                #변수 초기화
                temp = 55  # Initial temperature
                camera_fire = False
                camera_fire_first_time = 0  # Time when smoke was first detected 
                previous_camera_fire = False
                risk = 0
                
                detect_queue = deque()
                conf_queue = deque()
                
                recap = 0
                detect_fire = 0
                if recap > 5:
                    break
            # cv2.imwrite("out2.jpg", frame)
            results = model(frame, conf=0.3, classes=[1], verbose=False, imgsz=640)
            
            confidence = round(results[0].boxes.conf.mean().item() if results[0].boxes.conf.numel() > 0 else 0.0, 2)
            annotated_frame = results[0].plot()
            cv2.imwrite("out.jpg", annotated_frame)
   
            # if not is_show:
            # 	cv2.imwrite("out.jpg", annotated_frame)
            # 	is_show = True
            #fig = plt.figure()
            #fig.savefig('out.png', annotated_frame)
         
            
                       # 이미지 크기 가져오기
            height, width, _ = annotated_frame.shape
            # 온도값을 이미지 우측 하단에 표시
            temp_text = f"Temp: {temp}"
            text_position = (width - 150, height - 30)  # 우측 하단 위치 (x, y)
            cv2.putText(
                annotated_frame, 
                temp_text, 
                text_position, 
                cv2.FONT_HERSHEY_SIMPLEX, 
                0.7,  # 글자 크기
                (0, 0, 200),  # 빨간색
                2,  # 두께
                cv2.LINE_AA
            )
            # cv2.imwrite("out3.jpg", annotated_frame)
            # 이미지를 JPEG로 인코딩
            _, buffer = cv2.imencode('.jpg', annotated_frame)
            image_base64 = base64.b64encode(buffer).decode('utf-8')
            #cv2.imwrite("out4.jpg", image_base64)
            
            smoke_detected = 0  # Flag to check if smoke is detected
            for cls in results[0].boxes.cls:
                if cls == 1:
                    smoke_detected = 1
                    break
            
            detect_queue.append(smoke_detected)
            conf_queue.append(confidence)
            if len(detect_queue) == 5:
                _sum = sum(detect_queue)
                if not camera_fire and _sum >= 4: # 카메라는 불 났다고 판단
                    camera_fire = True
                elif camera_fire and _sum < 1: #카메라는 불이 이제 꺼졌다고 판단
                    camera_fire = False
                detect_queue.popleft()
                conf_queue.popleft()
            
            risk =  round(sum(conf_queue), 2)*15 + 10*(temp - 55)
            if risk > 100:
                risk = 100
                
            # Update temperature based on smoke detection
            if not previous_camera_fire:
                if camera_fire:
                    camera_fire_first_time = current_time  # Record the time when smoke is first detected
            else:
                elapsed_time = current_time - camera_fire_first_time
                if camera_fire == False or temp > 100:
                    if current_time - camera_fire_first_time > 300:
                        temp = 55
                        camera_fire_first_time = 0
                    pass
                else:
                    temp = 55 + int(elapsed_time)  # Limit to a maximum of 60

            if 80> temp > 65:
                if camera_fire:
                    detect_fire = 1
                else:
                    detect_fire = 0
            elif temp > 80:
                detect_fire = 1
                
            # kafka_dir["frame_offset"] = frame_key
            # kafka_dir["box_offset"] = results[0].boxes.xyxyn.tolist()
            # kafka_dir["class"] = results[0].boxes.cls.tolist()
            kafka_dir["camera_confidence"] = confidence
            kafka_dir["detect_fire"] = detect_fire
            kafka_dir["temperature"] = temp
            kafka_dir["risk"] = risk
            kafka_dir["fire_image"] = image_base64
            
            print(kafka_dir["camera_confidence"], kafka_dir["detect_fire"], kafka_dir["temperature"], kafka_dir["risk"])
            # with open('/home/aiot/rpi4_kafka_test/inference_out.txt', 'w') as file:
            #    file.write(', '.join([f"{key}: {value}" for key, value in direct.items() if key != 'fire_image']))
            json_producer.send("DGSP-FIRE-INFERENCE-SM1", value=kafka_dir)
            
            # Update last detection time
            last_detection_time = current_time
    if display == True:
       cv2.destroyAllWindows()
    


# python version에 따라 해당 코드가 필요할 수 있음 (3.11 이상?)
m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
setattr(m, 'range', range)
sys.modules['kafka.vendor.six.moves'] = m
bootstrap_servers=['piai_kafka2.aiot.town:9092']
# json 예시

json_producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers, # 전달하고자 하는 카프카 브로커의 주소 리스트
    value_serializer=lambda x:dumps(x).encode('utf-8'), # 메시지의 값 직렬화,
    client_id = "fire_inference",

)

# Temperature based detection
with open('/home/aiot/rpi4_kafka_test/gmm_params.pkl', 'rb') as file:
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


model = YOLO('/home/aiot/rpi4_kafka_test/best_train2_custom_finetune.pt')

detect_fire_temp_video(model, json_producer)
