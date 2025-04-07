import cv2
import av
import os
import time
import uuid
import queue
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from concurrent.futures import ThreadPoolExecutor


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

def capture_frames(frame_queue: queue.Queue, threshold_time):
    while True:
        cap = cv2.VideoCapture(0)
        if not cap.isOpened():
            print("Failed to open camera")
            continue
        cur_fps=cap.get(cv2.CAP_PROP_FPS)
        cur_frame_num=0
        start_time=time.time()
        while True:
            if ((time.time()-start_time)*cur_fps-cur_frame_num) > threshold_time * cur_fps:
                print("restart the camera")
                break
            ret, frame = cap.read()
            cur_frame_num+=1
            if not ret:
                print(f"cv2.VideoCapture error")
                break
            frame_queue.put(frame)
        cap.release()

def encode_and_send(frame_queue, server_ip, server_port, topic_name):
    producer = KafkaProducer(
        bootstrap_servers=f"{server_ip}:{server_port}"
    )
    cur_offset=get_last_offset(server_ip, server_port, topic_name)
    while True:
        cur_time=time.time()
        frames = []
        start_time = time.time()
        while time.time() - start_time < 5:
            try:
                frame = frame_queue.get(timeout=0.1)
                frames.append(frame)
            except queue.Empty:
                continue

        if not frames:
            continue

        output_file = str(uuid.uuid4()) + '.mp4'
        container = av.open(output_file, mode='w')
        stream = container.add_stream('h264', rate=30)
        stream.width = frames[0].shape[1]
        stream.height = frames[0].shape[0]
        stream.pix_fmt = 'yuv420p'
        stream.bit_rate = 1000000

        for frame in frames:
            av_frame = av.VideoFrame.from_ndarray(frame, format='bgr24')
            for packet in stream.encode(av_frame):
                container.mux(packet)

        for packet in stream.encode():
            container.mux(packet)

        container.close()

        with open(output_file, 'rb') as video_file:
            video_data = video_file.read()
            cur_offset+=1
            producer.send(topic=topic_name, key=str(cur_offset).encode('utf-8'), value=video_data)

        producer.flush()

        os.remove(output_file)
        print(f"encoding+producing time/cur_offset: {time.time()-cur_time}/{cur_offset}")


def produce_video_stream(server_ip, server_port, topic_name, threshold_time):
    frame_queue = queue.Queue()

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(capture_frames, frame_queue, threshold_time)
        executor.submit(encode_and_send, frame_queue, server_ip, server_port, topic_name)

if __name__=="__main__":
    produce_video_stream("piai_kafka.aiot.town", "9092", "TF-CAM-TEST", 5)
