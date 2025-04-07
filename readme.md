rpi_kafka_utility
=================   

kafka_produce.py
-------------------
## produce_camera_stream(server_ip, server_port, topic_name)
### server_ip: your kafka server's ip (or hostname)
### server_port: your kafka server's port
### topic_name: your assigned kafka topic
kafka_consume.py
-----------------------
## consume_camera_stream(server_ip, server_port, topic_name, group_id)
### server_ip: your kafka server's ip (or hostname)
### server_port: your kafka server's port
### topic_name: your assigned kafka topic
### group_id: your consumer group id
    1. our topic consider only one partition because of ordering, you have to use different group id
    2. if not, you can't use partition since it is already occupied by other consumer in same group
## store_kafka_stream_as_video(server_ip, server_port, topic_name, group_id, o_width, o_height, timing="current", fps=60, video_len=60, save_dir=".", save_name="test_video", width=None, height=None, p_num_offset_dict=None)
### server_ip: your kafka server's ip (or hostname)
### server_port: your kafka server's port
### topic_name: your assigned kafka topic
### group_id: your consumer group id
### o_width, o_height: original frame's width, height
    These at below are optional parameters
### current: reset offset if you need ("latest" for end offset and "earliest" for start offset)
### fps: your wanted frame per second (fps)
### video_len: your wanted video length (time unit is second)
### save_dir, save_name: your file will be store in path 'save_dir/save_name'
### width, height: you can change your frame's from 'o_width * o_height' to 'width * height'
### p_num_offset_dict: if you want to start your frame from specific partition's offset frame, you can give the value as {partition_num: offset_num}
    You can customize these things
### fps and length of video
### video file's directory and name
### pixel size of video's frame
### which frame to start video