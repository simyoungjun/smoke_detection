import subprocess
import os
import time
import uuid
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

FFMPEG_DIR='/home/aiot/jetson-ffmpeg/build/ffmpeg/ffmpeg'
OUTPUT_DIR='/home/aiot/rpi4_kafka_test/output_dir'

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

def produce_video(server_ip, server_port, topic_name, uuid_val):
    producer = KafkaProducer(
        bootstrap_servers=f"{server_ip}:{server_port}"
    )
    cur_offset=get_last_offset(server_ip, server_port, topic_name)
    cur_filenum=0
    while True:
        file_name=f"{uuid_val}_{cur_filenum}.mp4"
        if os.path.exists(f"{OUTPUT_DIR}/{file_name}") and os.path.exists(f"{OUTPUT_DIR}/{uuid_val}_{cur_filenum+1}.mp4"):
            with open(f"{OUTPUT_DIR}/{file_name}", "rb") as video_file:
                video_data=video_file.read()
                cur_offset+=1
                producer.send(topic=topic_name, key=str(cur_offset).encode('utf-8'), value=video_data)
                cur_filenum+=1
                os.remove(f"{OUTPUT_DIR}/{file_name}")
                time.sleep(4)
        else:
            time.sleep(0.1)

def produce_video_stream(server_ip, server_port, topic_name, segment_time):
    fps=15
    width=640
    height=480
    segment_time=segment_time
    uuid_val=str(uuid.uuid4())
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    command=[
            FFMPEG_DIR,
            "-f", "v4l2",
            "-framerate", f"{fps}",
            "-video_size", f"{width}x{height}",
            "-fflags", "nobuffer",
            "-i", "/dev/video0",
            "-c:v", "h264_nvmpi",
            "-b:v", "1000k",
            "-preset", "ultrafast",
            "-f", "segment",
            "-segment_time", f"{segment_time}",
            "-reset_timestamps", "1",
            "-flush_packets", "1",
            f"{OUTPUT_DIR}/{uuid_val}_%d.mp4"
            ]
    process=subprocess.Popen(command)

    produce_video(server_ip, server_port, topic_name, uuid_val)



if __name__=="__main__":
    produce_video_stream("piai_kafka.aiot.town", "9092", "TF-CAM-TEST", 5)
