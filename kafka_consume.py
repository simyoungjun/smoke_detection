import cv2
import numpy as np
from kafka import KafkaConsumer, TopicPartition
import ffmpeg
import time

def consume_camera_stream(server_ip, server_port, topic_name, group_id):
    consumer = KafkaConsumer(
        f"{topic_name}",
        bootstrap_servers=f"{server_ip}:{server_port}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=f"{group_id}",
        api_version=(2, 6, 0)
    )
    
    for message in consumer:
        frame_bytes = message.value

        np_arr = np.frombuffer(frame_bytes, dtype=np.uint8)
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

        if frame is not None:
            cv2.imshow('video', frame)
            #cv2.imwrite('png.png', frame)

            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
        else:
            print("Failed to decode frame")

    cv2.destroyAllWindows()

def store_kafka_stream_as_video(server_ip, server_port, topic_name, group_id, o_width, o_height, timing="current", fps=60, video_len=60, save_dir=".", save_name="test_video", width=None, height=None, p_num_offset_dict=None):
    def make_img_from_msg(msg_value):
        np_arr = np.frombuffer(msg_value, dtype=np.uint8)
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        return img
    
    frame_list=list()

    consumer=KafkaConsumer(
            f"{topic_name}",
            bootstrap_servers=f"{server_ip}:{server_port}",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=f"{group_id}",
            api_version=(2, 6, 0)
            )
    
    partitions = consumer.partitions_for_topic(f"{topic_name}")
    if not partitions:
        raise Exception(f"No partitions found for topic {topic_name}")
    
    topic_partitions = [TopicPartition(f"{topic_name}", p) for p in partitions]

    while not all(tp in consumer.assignment() for tp in topic_partitions):
        print("Waiting for partition assignment...")
        consumer.poll(0)
        time.sleep(1)

    if timing == "latest":
        for tp in topic_partitions:
            print(f"latest: {consumer.end_offsets([tp])[tp]}")
            consumer.seek(tp, consumer.end_offsets([tp])[tp])
    elif timing == "earliest":
        for tp in topic_partitions:
            consumer.seek(tp, consumer.beginning_offsets([tp]))
    
    if p_num_offset_dict != None:
        for idx, offset in p_num_offset_dict.items():
                consumer.seek(topic_partitions[idx], offset)
    
    current_offsets = {tp: consumer.position(tp) for tp in topic_partitions}
    print("Current Offsets:", current_offsets)

    while len(frame_list) < fps * video_len:
        print(f"current frame_list length: {len(frame_list)}")
        try:
            frame_list.append(make_img_from_msg(next(consumer).value))
        except StopIteration:
            message = consumer.poll(timeout_ms=1000)
            for _, messages in message.items():
                for msg in messages:
                    frame_list.append(make_img_from_msg(msg.value))
                    if len(frame_list) >= fps * video_len:
                        break
                if len(frame_list) >= fps * video_len:
                    break
    
    if len(frame_list) < fps * video_len:
        for tp, offset in current_offsets.items():
            consumer.seek(tp, offset)
            return None
    
    video_path = f"{save_dir}/{save_name}"
    
    if width != None and height != None:
        process = (
        ffmpeg
        .input('pipe:', format='rawvideo', pix_fmt='bgr24', s=f'{o_width}x{o_height}', r=fps)
        .filter('scale', width, height)
        .output(video_path, pix_fmt='yuv420p', vcodec='libx264', r=fps)
        .overwrite_output()
        .run_async(pipe_stdin=True)
        )
    else:
        process = (
        ffmpeg
        .input('pipe:', format='rawvideo', pix_fmt='bgr24', s=f'{o_width}x{o_height}', r=fps)
        .output(video_path, pix_fmt='yuv420p', vcodec='libx264', r=fps)
        .overwrite_output()
        .run_async(pipe_stdin=True)
        )
    
    for frame in frame_list:
        process.stdin.write(frame.astype(np.uint8).tobytes())
    
    process.stdin.close()
    process.wait()
    
    print(f"Video saved as {video_path}")

    

if __name__=="__main__":
    consume_camera_stream("piai_kafka.aiot.town", "9092", "TF-CAM-TEST", "webcam-group_tmp1")
    #store_kafka_stream_as_video("piai_kafka.aiot.town", "9092", "TF-CAM-DOOR2", "webcam-group2", 640, 480, "latest", 60, 120, ".", "second_test.mp4", 1280, 1080)
