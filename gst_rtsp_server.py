#!/usr/bin/env python3

import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstRtspServer', '1.0')
from gi.repository import Gst, GstRtspServer, GLib

class WebcamRTSPServer(GstRtspServer.RTSPMediaFactory):
    def __init__(self):
        super(WebcamRTSPServer, self).__init__()
        # GStreamer 파이프라인: Jetson 하드웨어 인코더(nvvidconv, omxh264enc)를 사용하여 H.264 인코딩
        self.set_launch("( v4l2src device=/dev/video0 ! video/x-raw, width=640, height=480, framerate=30/1 ! videoconvert ! video/x-raw, format=I420 ! nvvidconv ! omxh264enc insert-sps-pps=true bitrate=800000 ! rtph264pay name=pay0 pt=96 )")
        self.set_shared(True)

Gst.init(None)

# RTSP 서버 생성
server = GstRtspServer.RTSPServer()
factory = WebcamRTSPServer()

# 스트림 경로 지정
mounts = server.get_mount_points()
mounts.add_factory("/live", factory)

# RTSP 서버 시작
server.attach(None)
print("RTSP server is running at rtsp://127.0.0.1:8554/live")

# 메인 루프 실행
loop = GLib.MainLoop()
loop.run()
