import socket
import time
import paho.mqtt.client as paho


serial_device = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serial_device.settimeout(25)
serial_device.connect(("example.de", 80))

mqtt = paho.Client()
mqtt.connect("192.168.178.6", 1883, 60)
mqtt.loop_start()


frame_power_active = bytearray([0xa5,0x17,0x00,0x10,0x45,0xf7,0x00,0x70,0x23,0x4f,0xf8,0x02,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01,0x03,0x00,0x56,0x00,0x01,0x64,0x1a,0x18,0x15])

while True:
    try:
        serial_device.send(frame_power_active)
    except Exception:
        serial_device.close()
        raise('not connection')
    try:
        frame_in = serial_device.recv(64)
        power_active = int.from_bytes(frame_in[28:30], byteorder='big', signed=False)/10.0
        if power_active > 0.0 and power_active < 900.0: # realistische Werte
            mqtt.publish("home/deye/power_active", power_active)
        else:
            print('no data')
    except Exception:
	    mqtt.close()
	    serial_device.close()
	    raise('no data')
    time.sleep(30)
