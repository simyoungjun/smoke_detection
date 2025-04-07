import subprocess

ffmpeg_command = [
    "/usr/local/bin/ffmpeg",
    "-f", "v4l2",
    "-input_format", "yuyv422",
    "-framerate", "30",
    "-video_size", "1280x720",
    "-i", "/dev/video0",
    "-c:v", "mpeg4",
    "-b:v", "1000k",
    "-f", "rtsp",
    "-rtsp_transport", "tcp",
    "rtsp://192.168.50.83:8554/live"
]

try:
    process = subprocess.Popen(ffmpeg_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    print("FFmpeg Output:", stdout.decode())
    print("FFmpeg Error:", stderr.decode())
except Exception as e:
    print("An error occurred:", str(e))
