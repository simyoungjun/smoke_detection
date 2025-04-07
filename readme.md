
# On-Device Real-Time Fire Detection System for Industrial Dust Collectors

> Real-time monitoring and early fire detection in industrial dust collector systems using edge AI and sensor fusion.

---

## Overview

This project presents an on-device fire detection system designed for **industrial dust collectors** in a **metal spring manufacturing facility**. The system utilizes **RGB cameras**, **temperature sensors**, and **Jetson-based edge AI** to detect early signs of fire such as smoke and abnormal temperature rises.

---

## Core Features

- **Sensor Fusion Pipeline**
  - Real-time input from **RGB cameras** and **CT-1000N temperature sensors**
  - Data collected and streamed using **Kafka-Python**

- **Fire Detection AI Models**
  - **YOLOv8** for object detection of smoke
  - **GMM (Gaussian Mixture Model)** for detecting temperature anomalies
  - **Soft voting** fusion strategy for final fire detection decision

- **Edge Deployment**
  - **Jetson Orin Nano** executes detection on-device
  - Fire alert is triggered locally with minimal latency

---

##  Data Collection System

### Method Overview

| Source           | Description                                                                                 |
|------------------|---------------------------------------------------------------------------------------------|
| Camera           | RGB video recorded using Jetson Nano (resolution: **640×480**)                              |
| Temperature      | CT-1000N sensor logging every **1 second**                                                  |
| Storage Format   | `.avi` video files and `.csv` sensor logs                                                   |
| Data Streaming   | **Kafka-Python** is used to stream sensor data to an **external server**                    |

---

## Visual Examples

### 1. Smoke Detection with YOLOv8

<p align="center">
  <img src="images/smoke_detection_1.png" width="400"/>
</p>

Multiple bounding boxes for smoke with confidence scores.

---

### 2. On-Device Event Alert UI

<p align="center">
  <img src="images/fire_event_alert.png" width="400"/>
</p>

- Shows detected **smoke**, **temperature**, **sensor ID**, **risk level**
- All inference runs **on-device** with Jetson Nano



