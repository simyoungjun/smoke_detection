import cv2
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import time
import threading as Thread
import signal
import sys

FPS = 20
MAX_FPS = 20

class VideoGet:
    def __init__(self, src=0, fps=30):
        self.stream = cv2.VideoCapture(src)
        self.stream.set(cv2.CAP_PROP_FPS, fps)
        print(f"desired fps, real fps: {fps}, {self.stream.get(cv2.CAP_PROP_FPS)}")
        (self.grabbed, self.frame) = self.stream.read()
        print('width: {}, height : {}'.format(self.stream.get(3), self.stream.get(4)))
        if not self.stream.isOpened():
            print("Failed to open camera")
            self.stopped = True
        else:
            self.stopped = False

    def start(self):
        Thread.Thread(target=self.get, args=()).start()
        return self

    def get(self):
        while not self.stopped:
            if not self.grabbed:
                self.stop()
            else:
                (self.grabbed, self.frame) = self.stream.read()

    def stop(self):
        self.stopped = True

class VideoProcess:
    def __init__(self, frame=None, server_ip="piai_kafka.aiot.town", server_port="9092", topic_name="TF-CAM-TEST"):
        self.server_ip = server_ip
        self.server_port = server_port
        self.topic_name = topic_name
        self.producer = KafkaProducer(bootstrap_servers=f"{self.server_ip}:{self.server_port}")
        self.last_offset = get_last_offset(self.server_ip, self.server_port, self.topic_name)
        self.frame = frame
        self.stopped = False
        self.prev = 0
        self.frame_cnt = 0

    def start(self):
        Thread.Thread(target=self.process, args=()).start()
        return self

    def process(self):
        self.start_time = time.time()
        while not self.stopped:
            self.prev = time.time()
            if self.prev - self.start_time >= 1:
                print(f"fps: {self.frame_cnt}")
                self.frame_cnt = 0
                self.start_time = self.prev
            self.frame_cnt += 1
            _, buffer = cv2.imencode(".jpg", self.frame)
            frame_bytes = buffer.tobytes()
            self.last_offset += 1
            message_key = str(self.last_offset).encode('utf-8')
            self.producer.send(self.topic_name, key=message_key, value=frame_bytes)

    def stop(self):
        self.stopped = True

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
    video_getter = VideoGet(0, FPS).start()
    video_processor = VideoProcess(video_getter.frame, server_ip, server_port, topic_name).start()

    def signal_handler(sig, frame):
        print("Interrupt received, stopping...")
        video_getter.stop()
        video_processor.stop()
        video_getter.stream.release()
        video_processor.producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    while True:
        if video_getter.stopped or video_processor.stopped:
            video_getter.stop()
            video_processor.stop()
            video_getter.stream.release()
            video_processor.producer.close()
            break

        frame = video_getter.frame
        video_processor.frame = frame

if __name__ == "__main__":
    produce_camera_stream("piai_kafka.aiot.town", "9092", "TF-CAM-TEST")
