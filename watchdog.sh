# #!/bin/bash

# #_FFMPEG_PATH="/home/aiot/jetson-ffmpeg/build/ffmpeg/ffmpeg -f v4l2 -input_format yuyv422 -framerate 30 -video_size 1280x720 -i /dev/video0 -c:v mpeg4 -b:v 1000k -f rtsp -rtsp_transport tcp rtsp://100.71.100.88:8554/live"
# #_FFMPEG_PATH="ffmpeg -f v4l2 -input_format yuyv422 -framerate 30 -video_size 1280x720 -i /dev/video0 \
# #  -c:v libx264 -crf 23 -preset medium -g 30 -b:v 500k \
# #  -f rtsp -rtsp_transport tcp -max_delay 100000 rtsp://100.71.100.88:8554/live"
# _FFMPEG_PATH="ffmpeg -f v4l2 -input_format mjpeg -framerate 13 -video_size 800x600 -i /dev/video0 \
#   -c:v libx264 -crf 23 -preset medium -g 30 -b:v 500k \
#   -f rtsp -rtsp_transport tcp -max_delay 100000 rtsp://100.71.100.88:8554/live
# "
# _MEDIAMTX_PATH="/home/aiot/rpi4_kafka_test/mediamtx /home/aiot/rpi4_kafka_test/mediamtx.yml"

# _INFERENCE_PATH="/home/aiot/rpi4_kafka_test/inference_from_kafka_demo_v1.py"
# #_INFERENCE_PATH="/home/aiot/rpi4_kafka_test/dummy.py"

# function PID_CHECK() {
#     local name=$1
#     local command=$2
#     local time=$3
#     local pid_file="/tmp/${name}.pid"

#     if [ -f "$pid_file" ]; then
#         PID=$(cat "$pid_file")
#         if ps -p $PID > /dev/null; then
#             STATUS=$(ps -p $PID -o stat=)
#             if [[ "$STATUS" =~ ^[RS] ]]; then
#                 echo "[$name] Process is running with status: $STATUS."
#                 return
#             else
#                 echo "[$name] Process is in an abnormal state: $STATUS. Restarting..."
#             fi
#         else
#             echo "[$name] Process not running. Restarting..."
#         fi
#     else
#         echo "[$name] PID file not found. Starting process..."
#     fi
    
#     if [ "$name" == "inference" ]; then
#         source /home/aiot/yolo/bin/activate
#         command="python3 /home/aiot/rpi4_kafka_test/inference_from_kafka_demo_v1.py"
#         # command="python3 /home/aiot/rpi4_kafka_test/dummy.py"
#         nohup $command > /home/aiot/rpi4_kafka_test/logging/${name}_${time}.log 2>&1 &
#         echo $! > "$pid_file"
#     else
#         nohup $command > /home/aiot/rpi4_kafka_test/logging/${name}_${time}.log 2>&1 &
#         echo $! > "$pid_file"
#     fi
#     sleep 3
# }

# while true; do
#     sleep 3
# 	PID_CHECK "mediamtx" "$_MEDIAMTX_PATH" "$(date +%Y%m%d%H%M%S)"
# 	sleep 1
#     PID_CHECK "ffmpeg" "$_FFMPEG_PATH" "$(date +%Y%m%d%H%M%S)"

#     sleep 1
#     PID_CHECK "inference" "$_INFERENCE_PATH" "$(date +%Y%m%d%H%M%S)"
# done
