"""Estimated PV power from sun position -> MQTT home/heating/estpvpower.

Split out of diematic.py (same model, same topic), published once a minute.
"""
import datetime as dt
import logging
import math
import signal
from threading import Event

import numpy as np
import paho.mqtt.client as paho
import pytz
from pysolar import radiation
from pysolar.solar import get_altitude, get_azimuth
from systemd.journal import JournaldLogHandler

journald_handler = JournaldLogHandler()
journald_handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
logger = logging.getLogger('estpvpower')
logger.addHandler(journald_handler)
logger.setLevel(logging.INFO)

MQTT_HOST = '127.0.0.1'
MQTT_PORT = 1883
TOPIC = 'home/heating/estpvpower'
PERIOD = 60.0


def estpower(date):
    latitude = 50.303728
    longitude = 11.054529
    beta_pv = 70 / 180 * math.pi
    alpha_pv = 185 / 180 * math.pi
    area = 12.0 * 0.94

    beta_sun = get_altitude(latitude, longitude, date) / 180 * math.pi
    alpha_sun = get_azimuth(latitude, longitude, date) / 180 * math.pi

    pv = np.array([math.sin(alpha_pv), -math.cos(alpha_pv), math.sin(beta_pv)])
    sun = np.array([math.sin(alpha_sun), -math.cos(alpha_sun), math.sin(beta_sun)])

    cos_inc = np.dot(pv, sun) / np.linalg.norm(pv) / np.linalg.norm(sun)
    incident_angle = math.acos(max(-1.0, min(1.0, cos_inc))) * 180 / math.pi
    if 0 <= incident_angle <= 80:
        eta = 22 - (1 / (math.exp(0.0015 * (95 - incident_angle) ** 1.3) - 1))
    else:
        eta = 1
    return radiation.get_radiation_direct(date, beta_sun * 180 / math.pi) * area * eta / 100


def main():
    stop = Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    mqtt = paho.Client()
    mqtt.reconnect_delay_set(min_delay=1, max_delay=30)
    mqtt.connect_async(MQTT_HOST, MQTT_PORT, 60)
    mqtt.loop_start()
    logger.info('started')

    while True:
        try:
            value = estpower(dt.datetime.now(pytz.timezone('Europe/Berlin')))
            mqtt.publish(TOPIC, value)
        except Exception as e:
            logger.warning(f'estpower failed: {e}')
        if stop.wait(PERIOD):
            break

    mqtt.loop_stop()
    mqtt.disconnect()
    logger.info('stopped')


if __name__ == '__main__':
    main()
