from serial import Serial
import socket
import time
import paho.mqtt.client as paho
import numpy as np
from datetime import datetime as dt
from paho.mqtt.client import connack_string as ack

def on_connect(client, userdata, flags, rc, v5config=None):
    print(dt.now().strftime("%H:%M:%S.%f")[:-2] + " Connection returned result: "+ack(rc))
    client.subscribe("home/supla/devices/zamel-mew-01-e21028/channels/0/state/phases/1/power_active", qos=1)
    client.subscribe("home/supla/devices/zamel-mew-01-e21028/channels/0/state/phases/2/power_active", qos=1)
    client.subscribe("home/supla/devices/zamel-mew-01-e21028/channels/0/state/phases/3/power_active", qos=1)
    
def on_message(client, energy, message,tmp=None):
    global Ps1, Ps2, Ps3
    if message.topic == "home/supla/devices/zamel-mew-01-e21028/channels/0/state/phases/1/power_active":
        Ps1 = float(message.payload)
    if message.topic == "home/supla/devices/zamel-mew-01-e21028/channels/0/state/phases/2/power_active":
        Ps2 = float(message.payload)
    if message.topic == "home/supla/devices/zamel-mew-01-e21028/channels/0/state/phases/3/power_active":
        Ps3 = float(message.payload)
    
def on_subscribe(client, userdata, mid, qos,tmp=None):
    if isinstance(qos, list):
        qos_msg = str(qos[0])
    else:
        qos_msg = f"and granted QoS {qos[0]}"
    print(dt.now().strftime("%H:%M:%S.%f")[:-2] + " Subscribed " + qos_msg)   


serial_device = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serial_device.settimeout(4)
serial_device.connect(("192.168.178.47", 502))

mqtt = paho.Client()
mqtt.on_connect = on_connect
mqtt.on_subscribe = on_subscribe
mqtt.on_message = on_message

Ps1 = 0.0
Ps2 = 0.0
Ps3 = 0.0

frame_energy = bytearray([0x20, 0x23, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x20, 0x23, 0x00, 0x08])
frame_power = bytearray([0x20, 0x03, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x20, 0x03, 0x00, 0x08])
frame_powerfactor = bytearray([0x20, 0x1f, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x20, 0x1f, 0x00, 0x04])

mqtt.connect("192.168.178.6", 1883, 60)
mqtt.loop_start()


while True:
    try:
        serial_device.send(frame_energy)
    except:
        serial_device.close()
        mqtt.disconnect()
        raise Exception("Error sending frame_energy")
    try:
        frame_in = serial_device.recv(64)
    except:
        serial_device.close()
        mqtt.disconnect()
        raise Exception("Error receiving frame_energy")
    energy_import = int.from_bytes(frame_in[9:17], byteorder='big', signed=False)
    energy_export = int.from_bytes(frame_in[17:25], byteorder='big', signed=False)
    mqtt.publish("home/smartme/energy_import", energy_import/1000.0)
    mqtt.publish("home/smartme/energy_export", energy_export/1000.0)
    #time.sleep(1)
    #print(f'Energy import: {energy_import} Wh, frame_in[9:17]: {frame_in[9:17].hex()}')
    #print(f'Energy export: {energy_export} Wh, frame_in[17:25]: {frame_in[17:25].hex()}')
    #     	
    try:
        serial_device.send(frame_power)
    except:
        serial_device.close()
        mqtt.disconnect()
        raise Exception("Error sending frame_power")
    try:
        frame_in = serial_device.recv(64)
    except:
        serial_device.close()
        mqtt.disconnect()
        raise Exception("Error receiving frame_power")
    Pt = int.from_bytes(frame_in[9:13], byteorder='big', signed=True)
    P1 = int.from_bytes(frame_in[13:17], byteorder='big', signed=True)
    P2 = int.from_bytes(frame_in[17:21], byteorder='big', signed=True)
    P3 = int.from_bytes(frame_in[21:25], byteorder='big', signed=True)
    mqtt.publish("home/smartme/Pt", Pt/1000.0)
    mqtt.publish("home/smartme/P1", P1/1000.0)
    mqtt.publish("home/smartme/P2", P2/1000.0)
    mqtt.publish("home/smartme/P3", P3/1000.0)
    mqtt.publish("home/smartme/Pstorage", -(Ps1+Ps2+Ps3) + 15)
    #time.sleep(1)
    #print(f'Power Pt: {Pt} W, frame_in[9:13]: {frame_in[9:13].hex()}')
    #print(f'Power P1: {P1} W, frame_in[13:17]: {frame_in[13:17].hex()}')
    #print(f'Power P2: {P2} W, frame_in[17:21]: {frame_in[17:21].hex()}')
    #print(f'Power P3: {P3} W, frame_in[21:25]: {frame_in[21:25].hex()}')
    #     	
    if 0:
        try:
            serial_device.send(frame_powerfactor)
        except:
            serial_device.close()
            mqtt.disconnect()
            raise Exception("Error sending frame_power")
        try:
            frame_in = serial_device.recv(64)
        except:
            serial_device.close()
            mqtt.disconnect()
            raise Exception("Error receiving frame_power")
        CP1 = int.from_bytes(frame_in[9:11], byteorder='big', signed=True)
        CP2 = int.from_bytes(frame_in[11:13], byteorder='big', signed=True)
        CP3 = int.from_bytes(frame_in[13:15], byteorder='big', signed=True)
        mqtt.publish("home/smartme/CP1", CP1/1000.0)
        mqtt.publish("home/smartme/CP2", CP2/1000.0)
        mqtt.publish("home/smartme/CP3", CP3/1000.0)
        #time.sleep(1)
    #print(f'Power Pt: {Pt} W, frame_in[9:13]: {frame_in[9:13].hex()}')
    #print(f'Power P1: {P1} W, frame_in[13:17]: {frame_in[13:17].hex()}')
    #print(f'Power P2: {P2} W, frame_in[17:21]: {frame_in[17:21].hex()}')
    #print(f'Power P3: {P3} W, frame_in[21:25]: {frame_in[21:25].hex()}')
    
    #print(f'Received: "{frame_in.hex(" ")}"')


