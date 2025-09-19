import paho.mqtt.client as paho
from datetime import datetime, timezone
import pytz
import time

MQTT_HOST = "192.168.178.6"
MQTT_PORT = 1883
MQTT_TOPIC = "home/battery/batmin"
MQTT_TOPIC_SUNRISE = "home/openhab/sonneaufgegangen/state"
MQTT_TOPIC_SUNSET = "home/openhab/sonnenuntergang/state"

sunrise_time = None
sunset_time = None

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT with result code {rc}")
    client.subscribe([(MQTT_TOPIC_SUNRISE, 0), (MQTT_TOPIC_SUNSET, 0)])

def check_and_publish(client):
    """Checks if it's daytime and publishes the corresponding value."""
    if sunrise_time and sunset_time:
        now_utc = datetime.now(timezone.utc)
        if sunrise_time < now_utc < sunset_time:
            client.publish(MQTT_TOPIC, 0)
        else:
            client.publish(MQTT_TOPIC, 700)

def on_message(client, userdata, msg):
    global sunrise_time, sunset_time
    try:
        # The payload is a unix timestamp in milliseconds, e.g., 1666848600000
        payload_str = msg.payload.decode('utf-8')
        unix_ts_ms = int(payload_str)
        # Convert to a timezone-aware datetime object in UTC
        dt_obj = datetime.fromtimestamp(unix_ts_ms / 1000, tz=timezone.utc)

        # For printing, convert to the local timezone
        berlin_tz = pytz.timezone("Europe/Berlin")
        local_dt = dt_obj.astimezone(berlin_tz)

        if msg.topic == MQTT_TOPIC_SUNRISE:
            sunrise_time = dt_obj
            print(f"Sunrise time updated: {local_dt.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
        elif msg.topic == MQTT_TOPIC_SUNSET:
            sunset_time = dt_obj
            print(f"Sunset time updated: {local_dt.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
        
        # After updating, check and publish
        check_and_publish(client)
    except Exception as e:
        print(f"Error processing message on topic {msg.topic}: {e}")

if __name__ == "__main__":
    mqtt_client = paho.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    mqtt_client.loop_start()

    try:
        while True:
            # The main loop now just keeps the script alive.
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping publisher.")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()