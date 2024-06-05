import cv2
import numpy as np
from kafka import KafkaConsumer

def consume_camera_stream(server_ip, server_port, topic_name, group_id):
    consumer=KafkaConsumer(
            f"{topic_name}",
            bootstrap_servers=f"{server_ip}:{server_port}",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=f"{group_id}"
            )
    
    for message in consumer:
        frame_bytes=message.value

        np_arr=np.frombuffer(frame_bytes, dtype=np.uint8)
        frame=cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

        cv2.imshow('video',frame)

        if cv2.waitKey(1) & 0xFF==ord('q'):
            break
    cv2.destroyAllWindows()

if __name__=="__main__":
    consume_camera_stream("piai_kafka.aiot.town", "9092", "TF-CAM-DOOR1", "webcam-group")
