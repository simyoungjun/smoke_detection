import datetime
import time
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import json
import serial
def get_last_offset(server_ip, server_port, topic_name):
    #consumer = KafkaConsumer(bootstrap_servers=f'{server_ip}:{server_port}')
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

def modbus_routine(server_ip, server_port, topic_name):
    producer = KafkaProducer(bootstrap_servers=f"{server_ip}:{server_port}")

    client = ModbusClient(method='rtu', port='/dev/ttyUSB0', baudrate=19200,
                          parity='N', stopbits=1, bytesize=8, timeout=2)
    if not client.connect():
        print("Unable to create the libmodbus connection")
        print('Kafka Connect complete')
    client.write_register(1, 100)
    #last_offset = get_last_offset(server_ip, server_port, topic_name)

    # try:
    while True:
        response = client.read_holding_registers(address=40002, count=2, unit=1)
        print(response)
        if not response.isError():
            object_temp = response.registers[0] / 10.0
            ambient_temp = response.registers[1] / 10.0

            current_time_utc = datetime.datetime.utcnow()
            timestamp = int(current_time_utc.timestamp())
            
            #message_key = str(last_offset).encode('utf-8')
            data = {
                "timestamp": timestamp,
                "object_temperature": object_temp,
                "ambient_temperature": ambient_temp
            }
            value_data=json.dumps(data).encode('utf-8')
            # producer.send(topic_name, key=message_key, value=value_data)
            # producer.flush()
            
            #last_offset += 1
            print(object_temp)
            print(f'Data sent at {timestamp}: Object Temperature = {object_temp}, Ambient Temperature = {ambient_temp}')

    #         time.sleep(1)
    # finally:
    #     client.close()
    #     producer.close()

if __name__ == "__main__":
    modbus_routine("piai_kafka.aiot.town", "9092", "DGSP-FIRE-INFERENCE-SM1")
