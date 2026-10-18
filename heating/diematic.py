"""Diematic boiler readout (RS485 Modbus-RTU via TCP gateway) -> MQTT.

The boiler is bus master most of the time and only answers our requests in
its slave window, so a request is (re)sent whenever the bus has been quiet for
IDLE_GAP seconds, until it is answered or its deadline expires.

Robustness:
  * one loop owns the socket: no helper threads that can die or block silently
  * stream parser: frames are found in a receive buffer by address, function,
    length and CRC, so TCP fragmentation/coalescing does not lose frames
  * every error closes the socket and reconnects in-process with backoff; the
    gateway is restarted over HTTP only after repeated failures
  * systemd watchdog (WATCHDOG=1) and the MQTT heartbeat are sent only after a
    valid frame, so "connected but no data" is detected as well
  * retained /set messages are ignored and a write is skipped when the boiler
    already holds that value

The PV estimate (home/heating/estpvpower) lives in estpvpower.py.
"""
import logging
import os
import signal
import socket
import time
from threading import Event, Lock

import paho.mqtt.client as paho
import requests
from systemd.journal import JournaldLogHandler

journald_handler = JournaldLogHandler()
journald_handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
logger = logging.getLogger('diematic')
logger.addHandler(journald_handler)
logger.setLevel(logging.INFO)


# ============================================================
# Configuration
# ============================================================
GATEWAY_HOST = '192.168.1.7'
GATEWAY_PORT = 88
GATEWAY_RESTART_URL = 'http://192.168.1.7/success_en.html'
GATEWAY_RESTART_DATA = ("__SL_P_WRT=OK&__SL_P_WFT=NO"
                        "&__SL_P_WUN.value=admin&__SL_P_WUP.value=admin")
GATEWAY_RESTART_SETTLE = 10.0
GATEWAY_RESTART_AFTER_FAILS = 3   # consecutive failed sessions before HTTP restart

MQTT_HOST = '127.0.0.1'
MQTT_PORT = 1883
MQTT_TOPIC_ROOT = 'home/heating'

CONNECT_TIMEOUT = 10.0
RECV_POLL = 0.1                   # socket poll interval
IDLE_GAP = 0.7                    # bus quiet this long -> our turn to send
REQUEST_DEADLINE = 20.0           # give up on one request after this
NO_DATA_TIMEOUT = 60.0            # no valid frame for this long -> reconnect
WRITE_ATTEMPTS = 3
BACKOFF_MIN = 2.0
BACKOFF_MAX = 60.0
RX_BUFFER_MAX = 4096

ADDRESS = 0x0A
READ_HOLDING = 0x03
WRITE_MULTIPLE = 0x10

# register blocks read round-robin
READ_BLOCKS = [
    {'adr': 7,   'nb': 14},
    {'adr': 89,  'nb': 8},
    {'adr': 59,  'nb': 4},
    {'adr': 427, 'nb': 33},
]

# register -> topic name, scale
PUBLISH = {
    7:   {'nm': 'exttemp',           'sc': 10},
    8:   {'nm': 'sumwintemp',        'sc': 10},
    9:   {'nm': 'antifreezeexttemp', 'sc': 10},
    13:  {'nm': 'daysantifreeze',    'sc': 1},
    14:  {'nm': 'daytemp',           'sc': 10},
    15:  {'nm': 'nighttemp',         'sc': 10},
    16:  {'nm': 'antifreezetemp',    'sc': 10},
    17:  {'nm': 'mode',              'sc': 1},
    59:  {'nm': 'watertempset',      'sc': 10},
    96:  {'nm': 'watertempnight',    'sc': 10},
    438: {'nm': 'largetemp',         'sc': 10},
    451: {'nm': 'ionizationcurrent', 'sc': 10},
    452: {'nm': 'supplytemp',        'sc': 10},
    453: {'nm': 'returntemp',        'sc': 10},
    454: {'nm': 'exhausttemp',       'sc': 10},
    455: {'nm': 'rpm',               'sc': 1},
    456: {'nm': 'pressure',          'sc': 10},
    459: {'nm': 'watertemp',         'sc': 10},
}
REG_IONIZATION = 451
REG_RPM = 455

# /set/<name> -> register, scale
WRITABLE = {
    'sumwin':         (8,  10),
    'daytemp':        (14, 10),
    'nighttemp':      (15, 10),
    'mode':           (17, 1),
    'watertempset':   (59, 10),
    'telecmd1':       (94, 1),
    'watertempnight': (96, 10),
}


def sd_notify(state):
    """Minimal sd_notify(3); no-op when not started by systemd."""
    addr = os.environ.get('NOTIFY_SOCKET')
    if not addr:
        return
    if addr.startswith('@'):
        addr = '\0' + addr[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.connect(addr)
            s.sendall(state.encode())
    except OSError as e:
        logger.warning(f'sd_notify failed: {e}')


# ============================================================
# Modbus helpers
# ============================================================
def calc_crc(data):
    crc = 0xFFFF
    for pos in data:
        crc ^= pos
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def with_crc(frame):
    crc = calc_crc(frame)
    return bytes(frame) + bytes([crc & 0xFF, (crc >> 8) & 0xFF])


def read_request(adr, nb):
    # trailing 0x00 kept from the original implementation (gateway framing)
    return with_crc([ADDRESS, READ_HOLDING, adr >> 8, adr & 0xFF, nb >> 8, nb & 0xFF]) + b'\x00'


def write_request(adr, values):
    frame = [ADDRESS, WRITE_MULTIPLE, adr >> 8, adr & 0xFF, 0x00, len(values), 2 * len(values)]
    for v in values:
        frame += [(v >> 8) & 0xFF, v & 0xFF]
    return with_crc(frame) + b'\x00'


def write_ack(adr, count):
    return with_crc([ADDRESS, WRITE_MULTIPLE, adr >> 8, adr & 0xFF, 0x00, count])


def decode(raw):
    """Diematic uses sign-magnitude for negative values."""
    return -float(raw & 0x7FFF) if raw >= 0x8000 else raw


def extract_frames(buf, read_byte_counts):
    """Pull complete, CRC-valid frames addressed from the boiler out of buf.

    Returns (frames, consumed). Bytes that cannot start one of our frames are
    skipped; an incomplete candidate at the end stays in the buffer.
    """
    frames = []
    i = 0
    n = len(buf)
    while i < n:
        if buf[i] != ADDRESS:
            i += 1
            continue
        if i + 1 >= n:
            break                                   # 0x0A at the end: wait
        func = buf[i + 1]
        if func == READ_HOLDING:
            if i + 2 >= n:
                break
            count = buf[i + 2]
            if count not in read_byte_counts:
                i += 1
                continue
            end = i + 5 + count
        elif func == WRITE_MULTIPLE:
            end = i + 8
        else:
            i += 1
            continue
        if end > n:
            break                                   # wait for the rest
        body = buf[i:end - 2]
        if calc_crc(body) == buf[end - 2] | (buf[end - 1] << 8):
            frames.append(bytes(buf[i:end]))
            i = end
        else:
            i += 1
    return frames, i


# ============================================================
# Diematic
# ============================================================
class Diematic:
    def __init__(self, stop, mqtt, topic_root=MQTT_TOPIC_ROOT,
                 host=GATEWAY_HOST, port=GATEWAY_PORT):
        self.stop = stop
        self.mqtt = mqtt
        self.topic_root = topic_root
        self.host = host
        self.port = port
        self.sock = None
        self.read_byte_counts = {b['nb'] * 2: b for b in READ_BLOCKS}
        self.registers = {}               # last raw value read per register
        self._lock = Lock()
        self._pending_writes = {}         # register -> raw value (latest wins)
        self._next_read = 0
        self._current = None              # outstanding request

    # ---------- called from the MQTT thread ----------
    def queue_write(self, name, payload):
        reg, scale = WRITABLE[name]
        raw = int(round(float(payload) * scale)) if scale != 1 else int(payload)
        with self._lock:
            known = self.registers.get(reg)
            if known == raw:
                logger.info(f'set/{name}={payload}: boiler already has this value, skipped')
                self._pending_writes.pop(reg, None)
                return
            self._pending_writes[reg] = raw
        logger.info(f'set/{name}={payload}: write of register {reg}={raw} queued')

    # ---------- request scheduling ----------
    def _next_request(self):
        with self._lock:
            if self._pending_writes:
                reg, raw = next(iter(self._pending_writes.items()))
                del self._pending_writes[reg]
                return {'kind': 'write', 'reg': reg, 'raw': raw,
                        'frame': write_request(reg, [raw]), 'ack': write_ack(reg, 1),
                        'attempt': 1, 'started': time.monotonic()}
        block = READ_BLOCKS[self._next_read]
        self._next_read = (self._next_read + 1) % len(READ_BLOCKS)
        return {'kind': 'read', 'block': block,
                'frame': read_request(block['adr'], block['nb']),
                'started': time.monotonic()}

    def _request_expired(self, now):
        req = self._current
        if req['kind'] == 'write' and req['attempt'] < WRITE_ATTEMPTS:
            logger.warning(f"write register {req['reg']} not acknowledged, retry {req['attempt'] + 1}")
            req['attempt'] += 1
            req['started'] = now
            return
        if req['kind'] == 'write':
            logger.warning(f"write register {req['reg']} failed after {WRITE_ATTEMPTS} attempts, dropped")
        else:
            logger.info(f"read block {req['block']['adr']} not answered within {REQUEST_DEADLINE:.0f}s")
        self._current = self._next_request()

    # ---------- frame handling ----------
    def _handle_frame(self, frame):
        """Returns True for a valid answer to one of our requests."""
        if frame[1] == WRITE_MULTIPLE:
            req = self._current
            if req and req['kind'] == 'write' and frame == req['ack']:
                logger.info(f"write register {req['reg']}={req['raw']} acknowledged")
                with self._lock:
                    self.registers[req['reg']] = req['raw']
                self._current = self._next_request()
                return True
            return False
        block = self.read_byte_counts[frame[2]]
        values = [(frame[3 + 2 * k] << 8) | frame[4 + 2 * k] for k in range(block['nb'])]
        self._publish_block(block['adr'], values)
        req = self._current
        if req and req['kind'] == 'read' and req['block'] is block:
            self._current = self._next_request()
        return True

    def _publish_block(self, adr, values):
        with self._lock:
            for k, raw in enumerate(values):
                self.registers[adr + k] = raw
        poweron = False
        for k, raw in enumerate(values):
            reg = adr + k
            item = PUBLISH.get(reg)
            if item:
                value = decode(raw)
                if item['sc'] != 1:
                    value = value / item['sc']
                self.mqtt.publish(f"{self.topic_root}/{item['nm']}", round(value, 2))
            if reg == REG_IONIZATION and raw > 0:
                poweron = True
            if reg == REG_RPM:
                power = raw / 5900 * 24.5 * 0.61 if poweron else 0.0
                self.mqtt.publish(f'{self.topic_root}/power', round(power, 2))

    def _alive(self):
        sd_notify('WATCHDOG=1')
        self.mqtt.publish(f'{self.topic_root}/heartbeat', int(time.time()), retain=True)

    # ---------- connection ----------
    def restart_gateway(self):
        logger.info('Restart Modbus gateway...')
        try:
            res = requests.post(GATEWAY_RESTART_URL,
                                headers={'Authorization': 'Basic YWRtaW46YWRtaW4='},
                                data=GATEWAY_RESTART_DATA, timeout=5)
            logger.info(f'gateway restart answered {res.status_code}')
        except requests.RequestException as e:
            logger.warning(f'gateway restart failed: {e}')
            return
        self.stop.wait(GATEWAY_RESTART_SETTLE)

    def _connect(self):
        self.sock = socket.create_connection((self.host, self.port), CONNECT_TIMEOUT)
        self.sock.settimeout(RECV_POLL)
        logger.info(f'connected to {self.host}:{self.port}')

    def _close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def _session(self):
        """Runs until stop or an error; returns True if valid data was received."""
        buf = bytearray()
        now = time.monotonic()
        last_rx = last_tx = now
        last_valid = now
        got_data = False
        self._current = self._next_request()
        while not self.stop.is_set():
            try:
                data = self.sock.recv(1024)
                if not data:
                    raise ConnectionError('gateway closed the connection')
                buf.extend(data)
                last_rx = time.monotonic()
            except socket.timeout:
                pass
            now = time.monotonic()

            if buf:
                frames, used = extract_frames(buf, self.read_byte_counts)
                del buf[:used]
                if len(buf) > RX_BUFFER_MAX:
                    del buf[:-RX_BUFFER_MAX // 2]
                for frame in frames:
                    if self._handle_frame(frame):
                        last_valid = now
                        got_data = True
                        self._alive()

            if now - last_valid > NO_DATA_TIMEOUT:
                raise TimeoutError(f'no valid answer for {NO_DATA_TIMEOUT:.0f}s')
            if now - self._current['started'] > REQUEST_DEADLINE:
                self._request_expired(now)
            if now - last_rx >= IDLE_GAP and now - last_tx >= IDLE_GAP:
                self.sock.sendall(self._current['frame'])
                last_tx = now
        return got_data

    def run(self):
        fails = 0
        backoff = BACKOFF_MIN
        while not self.stop.is_set():
            if fails and fails % GATEWAY_RESTART_AFTER_FAILS == 0:
                self.restart_gateway()
            got_data = False
            try:
                self._connect()
                got_data = self._session()
            except (OSError, ConnectionError, TimeoutError) as e:
                logger.warning(f'connection error: {e}')
            except Exception as e:
                logger.exception(f'unexpected error: {e}')
            finally:
                self._close()
            if got_data:
                fails, backoff = 0, BACKOFF_MIN
            else:
                fails += 1
            if self.stop.wait(backoff):
                break
            backoff = min(BACKOFF_MAX, backoff * 2)
        logger.info('stopped')


# ============================================================
# MQTT
# ============================================================
def on_connect_mqtt(client, diematic, flags, rc):
    logger.info(f'MQTT connected {rc}')
    client.subscribe(f'{diematic.topic_root}/set/#')


def on_disconnect_mqtt(client, diematic, rc):
    logger.info(f'MQTT disconnected {rc}')


def on_message_mqtt(client, diematic, msg):
    if msg.retain:
        # replayed on every (re)subscribe: never re-send old commands
        logger.info(f'ignored retained {msg.topic} {msg.payload}')
        return
    name = msg.topic.rsplit('/', 1)[-1]
    if name not in WRITABLE:
        return
    try:
        diematic.queue_write(name, msg.payload.decode().strip())
    except ValueError as e:
        logger.warning(f'bad payload on {msg.topic}: {msg.payload} ({e})')


def main():
    logger.info('init')
    stop = Event()

    def handle_signal(*_):
        logger.info('stop signal received')
        stop.set()
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    mqtt = paho.Client()
    diematic = Diematic(stop, mqtt)
    mqtt.user_data_set(diematic)          # before connect: no on_connect race
    mqtt.on_connect = on_connect_mqtt
    mqtt.on_message = on_message_mqtt
    mqtt.on_disconnect = on_disconnect_mqtt
    mqtt.reconnect_delay_set(min_delay=1, max_delay=30)
    mqtt.connect_async(MQTT_HOST, MQTT_PORT, 60)
    mqtt.loop_start()

    diematic.run()

    mqtt.loop_stop()
    mqtt.disconnect()
    logger.info('End of diematic.')


if __name__ == '__main__':
    main()
