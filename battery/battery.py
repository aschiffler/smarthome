#!/usr/bin/env python3
"""Battery + heater controller.

PI controller against the smart-me grid meter, read directly via Modbus TCP
(no dependency on the MQTT broker for the control input), talking to a Juntek
charger and a Soyo inverter via Modbus-RTU-over-TCP, and driving a heater
through an I2C DAC. Meter readings are re-published on home/smartme/*.

Re-worked for stability and cycle time:
  * single reconnecting control thread (no nested Thread/Thread.run() restart)
  * non-blocking I/O loop: no fixed time.sleep() between sends; relies on recv
  * exact-length recv with stale-byte drain at the start of each cycle
  * back-calculation anti-windup PI, integral reset on enable rising edge
  * conditional DAC writes (only when value changes)
  * Modbus restart HTTP call has a timeout
  * write-frame selection by name, not by linear scan
  * shared MQTT state guarded by a single lock; the control thread snapshots
    once per cycle
"""

import logging
import os
import signal
import socket
import struct
import time
from collections import deque
from datetime import datetime
from enum import Enum
from threading import Event, Lock, Thread

import numpy as np
import paho.mqtt.client as paho
import requests
from requests.auth import HTTPBasicAuth
from smbus2 import SMBus
from systemd import journal


# ============================================================
# Configuration
# ============================================================
MODBUS_HOST = '192.168.0.151'
MODBUS_PORT = 8888
MODBUS_LOGIN = ('admin', 'admin')
MODBUS_HTTP_TIMEOUT = 5.0          # HTTP request timeout (was: no timeout -> hang)
MODBUS_SETTLE_AFTER_RESTART = 8.0  # was 10s blocking sleep

MQTT_BROKER = '192.168.1.6'
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
MQTT_TOPIC_ROOT = 'home/battery'
MQTT_CLIENT_ID = 'battery-control'

# smart-me grid meter (Modbus TCP). The meter serves only ONE client at a
# time, so smartme.service on "home" must be disabled while this runs.
METER_HOST = '192.168.1.189'
METER_PORT = 502
METER_TIMEOUT = 2.0                # per-read timeout (meter answers in ~200 ms)
METER_RECONNECT_BACKOFF = 2.0
METER_TOPIC_ROOT = 'home/smartme'
STORAGE_TOPIC_BASE = 'home/supla/devices/zamel-mew-01-e21028/channels/0/state/phases'

I2C_BUS_NUM = 1
DAC_ADDR = 0x60
DAC_VMAX = 4.55
HEATER_DAC_GAIN = 0.85             # final scaling factor on heater DAC

LOOP_PERIOD = 0.1                  # target control-loop period (s)
SOCKET_TIMEOUT = 3.0               # per-recv timeout (s)
PUBLISH_PERIOD = 1.0               # MQTT publish throttle (s)
RECONNECT_BACKOFF = 5.0            # delay before reconnecting after a failure
MAX_CONSECUTIVE_TIMEOUTS = 3       # tolerate this many consecutive timeouts before reconnecting

SENSOR_STALE_AFTER = 5.0
HEATER_TEMP_STALE_AFTER = 60.0     # was 20s -> too short, caused chatter
HEATER_TEMP_MAX = 70.0

# IIR filter on the power-sensor input. Off by default: the filter adds phase
# delay which can destabilise the PI loop. Only enable it if you understand
# the trade-off (more noise rejection vs. slower response / lower phase margin).
INPUT_FILTER_ENABLED = False

POWER_LIMIT_DEFAULT = 700.0
POWER_LIMIT_CHARGE_DEFAULT = 1200.0
POWER_LIMIT_HEAT = 1300.0
POWER_EPS = 15.0
POWER_EPS_HEATER_ONLY = 50.0       # hysteresis to leave CHARGE_HEATER_ONLY
CHARGE_CURRENT_MAX = 40.0
CHARGE_EFF = 0.95

# !!! Battery safety limits -- review for your actual pack !!!
# The original code had voltage_lower_limit = -1.0 which effectively disabled
# the low-voltage cutoff and the EMPTY-state detection. Set these to the
# real numbers for your pack before deploying.
VOLTAGE_LOWER_LIMIT = -1.0
VOLTAGE_HYSTERESIS = 1.8
VOLTAGE_UPPER_LIMIT = 28.6

KP_DEFAULT = 0.2
KI_DEFAULT = 0.25

START_DELAY = 2.0                  # seconds in IDLE before transition to ON
CHARGE_DONE_CURRENT = 2.0          # A below which we declare battery FULL
CHARGE_FULL_VOLTAGE_MARGIN = 0.5   # V below VOLTAGE_UPPER_LIMIT; must be reached before FULL is declared


# ============================================================
# Logging
# ============================================================
if os.environ.get("JOURNAL_STREAM"):
    # running under systemd: send directly to journald
    _handlers = [journal.JournalHandler()]
else:
    # running from command line: print to stdout
    _handlers = [logging.StreamHandler()]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=_handlers,
)
log = logging.getLogger("battery")


# ============================================================
# IIR live filter (4th-order, 0.4*wn cutoff)
# ============================================================
class LiveLFilter:
    def __init__(
        self,
        b=np.array([0.43284664499029185, 1.7313865799611674, 2.597079869941751,
                    1.7313865799611674, 0.43284664499029185]),
        a=np.array([1.0, 2.369513007182037, 2.313988414415879,
                    1.0546654058785672, 0.18737949236818482]),
    ):
        self.b = b
        self.a = a
        self._xs = deque([0.0] * len(b), maxlen=len(b))
        self._ys = deque([0.0] * (len(a) - 1), maxlen=len(a) - 1)

    def __call__(self, x):
        if np.isnan(x):
            return x
        self._xs.appendleft(float(x))
        y = float(np.dot(self.b, self._xs) - np.dot(self.a[1:], self._ys))
        y /= self.a[0]
        self._ys.appendleft(y)
        return y


# ============================================================
# Modbus CRC-16
# ============================================================
def calc_crc(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


# ============================================================
# State enums
# ============================================================
class State(Enum):
    IDLE = 0
    DISCHARGE = 1
    CHARGE = 2
    ON = 3
    CHARGE_HEATER_ONLY = 4
    ERROR = -1


class BatteryState(Enum):
    INIT = -1
    EMPTY = 0
    FULL = 1
    OK = 2


# ============================================================
# Signal handling
# ============================================================
def install_signal_handlers(stop_signal):
    def handler(*_):
        log.info("Stop signal received")
        stop_signal.set()
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


# ============================================================
# Protocol constants
# ============================================================
JUNTEK_R01_REQ = bytes([0x02, 0x03, 0x10, 0x00, 0x00, 0x03, 0x01, 0x38, 0x00])
JUNTEK_R01_HDR = bytes([0x02, 0x03, 0x06])
JUNTEK_R01_LEN = 3 + 6 + 2          # slave/func/bytecount + 6 data + 2 CRC

JUNTEK_SWITCH_OFF = bytes([0x02, 0x06, 0x00, 0x02, 0x00, 0x00, 0x28, 0x39])
JUNTEK_SWITCH_ON  = bytes([0x02, 0x06, 0x00, 0x02, 0x00, 0x01, 0xe9, 0xf9])
JUNTEK_ECHO_LEN = 8                 # Modbus RTU write echo
JUNTEK_OFF_ECHO_PFX = bytes([0x02, 0x06, 0x00, 0x02])
JUNTEK_CHRG_FRAME_HDR = bytes([0x02, 0x06, 0x00, 0x01])

SOYO_OFF = bytes([0x24, 0x56, 0x00, 0x21, 0x00, 0x00, 0x80, 0x08])

# smart-me: one read of holding registers 0x2003..0x202A (40 regs = 80 bytes)
#   0x2003..0x200A  power   Pt, P1, P2, P3 (4 x int32, mW)
#   0x2023..0x202A  energy  import, export (2 x uint64, Wh*1000)
METER_REQ = bytes([0x20, 0x03, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03,
                   0x20, 0x03, 0x00, 0x28])
METER_RESP_LEN = 9 + 80             # MBAP(7) + fcode + bytecount + 80 data
METER_OFF_POWER = 9
METER_OFF_ENERGY = 9 + (0x2023 - 0x2003) * 2


# ============================================================
# BatteryControl
# ============================================================
class BatteryControl(Thread):
    def __init__(self, stop_signal, host, port, mqtt_client,
                 mqtt_topic_root=MQTT_TOPIC_ROOT, timeout=SOCKET_TIMEOUT):
        super().__init__(name="BatteryControl", daemon=True)
        self.stop_signal = stop_signal
        self.host = host
        self.port = port
        self.timeout = timeout
        self.mqtt = mqtt_client
        self.mqtt_topic_root = mqtt_topic_root

        # ---- Shared state (MQTT writes, control reads). Guarded by _lock. ----
        self._lock = Lock()
        self._sensor_filtered = 0.0
        self._sensor_last = datetime.now()
        self._heater_temp = 0.0
        self._heater_temp_last = datetime.now()
        self._enable_req = 0
        self._enable_prev = 0
        self._kp = KP_DEFAULT
        self._ki = KI_DEFAULT
        self._ovr = 0
        self._power_limit = POWER_LIMIT_DEFAULT
        self._power_limit_charge = POWER_LIMIT_CHARGE_DEFAULT
        self._input_filter = LiveLFilter() if INPUT_FILTER_ENABLED else None
        self._battery_state_ovr = None   # set from MQTT, consumed once per cycle

        # ---- Control-thread-only state ----
        self.power_setpoint = 0.0
        self.power_setpoint_integral = 0.0
        self._power_limit_charge_snap = POWER_LIMIT_CHARGE_DEFAULT
        self.charger_state = 0
        self.voltage = 26.0
        self.charge_current = 0.0
        self.heartbeat = 0
        self.state = State.IDLE
        self.battery_state = BatteryState.INIT
        self.power_eps_discharge = POWER_EPS
        self.last_start = datetime.now()
        self.last_publish = datetime.min
        self.heater_voltage = 0.0
        self._last_cycle_t = time.monotonic()

        # Per-cycle transmit flags
        self.tx_juntek_switch_off = False
        self.tx_juntek_switch_on = False
        self.tx_juntek_charge = False
        self.tx_soyo_discharge = False
        self.tx_soyo_off = False

        # Hardware
        self.auth_modbus = HTTPBasicAuth(*MODBUS_LOGIN)
        self.dac_vmax = DAC_VMAX
        self.i2c_bus = SMBus(I2C_BUS_NUM)
        self._last_dac = -1
        self._dac_write_raw(0)

        self.sock = None

    # ---------- MQTT-thread setters ----------
    def set_power_sensor(self, watts):
        raw = float(watts)
        value = self._input_filter(raw) if self._input_filter is not None else raw
        with self._lock:
            biased = value + 5.0
            if self.state in (State.CHARGE, State.CHARGE_HEATER_ONLY):
                biased += 25.0
            self._sensor_filtered = biased
            self._sensor_last = datetime.now()

    def set_heater_temp(self, t):
        with self._lock:
            self._heater_temp = float(t)
            self._heater_temp_last = datetime.now()

    def set_enable(self, v):
        with self._lock:
            self._enable_req = int(v)

    def set_kp(self, v):
        with self._lock:
            self._kp = float(v)
        log.info(f"kp={v}")

    def set_ki(self, v):
        with self._lock:
            self._ki = float(v)
        log.info(f"ki={v}")

    def set_ovr(self, v):
        with self._lock:
            self._ovr = int(v)

    def set_power_limit(self, w):
        with self._lock:
            self._power_limit = max(0.0, min(700.0, float(w)))

    def set_power_limit_charge(self, w):
        with self._lock:
            self._power_limit_charge = max(0.0, min(1200.0, float(w)))

    def set_battery_state(self, v):
        state = BatteryState(int(v))
        with self._lock:
            self._battery_state_ovr = state
        log.info(f"battery_state override requested: {state}")

    # ---------- Snapshot once per cycle ----------
    def _snapshot(self):
        now = datetime.now()
        with self._lock:
            enable_now = self._enable_req
            rising = enable_now == 1 and self._enable_prev == 0
            self._enable_prev = enable_now
            bat_ovr = self._battery_state_ovr
            self._battery_state_ovr = None   # consume once
            return {
                'now': now,
                'enable': enable_now,
                'enable_rising': rising,
                'sensor': self._sensor_filtered,
                'sensor_age': (now - self._sensor_last).total_seconds(),
                'heater_temp': self._heater_temp,
                'heater_temp_age': (now - self._heater_temp_last).total_seconds(),
                'kp': self._kp,
                'ki': self._ki,
                'ovr': self._ovr,
                'power_limit': self._power_limit,
                'power_limit_charge': self._power_limit_charge,
                'battery_state_ovr': bat_ovr,
            }

    # ---------- Socket I/O ----------
    def _connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((self.host, self.port))
        log.info(f"Connected to {self.host}:{self.port}")

    def _close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def _drain(self):
        """Discard any stale bytes already sitting in the socket buffer."""
        if self.sock is None:
            return
        self.sock.setblocking(False)
        try:
            while True:
                d = self.sock.recv(4096)
                if not d:
                    break
        except (BlockingIOError, OSError):
            pass
        finally:
            self.sock.setblocking(True)
            self.sock.settimeout(self.timeout)

    def _recv_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("socket closed by peer")
            buf.extend(chunk)
        return bytes(buf)

    # ---------- Modbus gateway restart ----------
    def restart_modbus(self):
        url = f'http://{self.host}/login.cgi?restart'
        try:
            requests.get(url, auth=self.auth_modbus, timeout=MODBUS_HTTP_TIMEOUT)
        except requests.RequestException as e:
            log.warning(f"Restart Modbus HTTP failed: {e}")
            return False
        log.info("Modbus restart requested, settling...")
        if self.stop_signal.wait(MODBUS_SETTLE_AFTER_RESTART):
            return False
        self.last_start = datetime.now()
        log.info("Modbus restarted")
        return True

    # ---------- Protocol ----------
    def _read_juntek_r01(self):
        self._drain()
        self.sock.sendall(JUNTEK_R01_REQ)
        data = self._recv_exact(JUNTEK_R01_LEN)
        if data[:3] != JUNTEK_R01_HDR:
            raise ValueError(f"bad r01 header: {data.hex()}")
        self.charger_state = data[4]
        self.voltage = ((data[5] << 8) | data[6]) / 100.0
        self.charge_current = ((data[7] << 8) | data[8]) / 1000.0

    def _juntek_charge_frame(self):
        if self.voltage > 0.0:
            current = -self.power_setpoint / self.voltage / CHARGE_EFF
            max_by_power = self._power_limit_charge_snap / self.voltage
            current = min(current, max_by_power)
        else:
            current = 0.5  # nudge BMS out of discharge protection
        current = max(0.0, min(CHARGE_CURRENT_MAX, current))
        ma = int(current * 1000)
        frame = bytearray(JUNTEK_CHRG_FRAME_HDR)
        frame.append((ma >> 8) & 0xff)
        frame.append(ma & 0xff)
        crc = calc_crc(frame)
        frame.append(crc & 0xff)
        frame.append((crc >> 8) & 0xff)
        return bytes(frame)

    def _soyo_discharge_frame(self):
        if self.voltage > 0.0:
            sp = max(0, int(self.power_setpoint))
            hb = (sp >> 8) & 0xff
            lb = sp & 0xff
        else:
            hb = lb = 0
        cs = (264 - hb - lb) & 0xff  # preserved from original firmware
        return bytes([0x24, 0x56, 0x00, 0x21, hb, lb, 0x80, cs])

    def _send_juntek_with_echo(self, frame, expect_prefix=None):
        self.sock.sendall(frame)
        try:
            data = self._recv_exact(JUNTEK_ECHO_LEN)
        except socket.timeout:
            log.debug("juntek echo timeout (tolerated)")
            return
        if expect_prefix and not data.startswith(expect_prefix):
            log.debug(f"juntek echo mismatch: {data.hex()}")

    def _send_soyo(self, frame):
        # Soyo doesn't reply. Any inbound bytes get drained next cycle.
        self.sock.sendall(frame)

    # ---------- PI with back-calculation anti-windup ----------
    def _pi_update(self, snap):
        if snap['enable'] != 1:
            self.power_setpoint = 0.0
            self.power_setpoint_integral = 0.0
            return
        if snap['enable_rising']:
            self.power_setpoint_integral = 0.0
            log.info("Enable rising edge -> integral reset")
        if snap['sensor_age'] > SENSOR_STALE_AFTER:
            log.info(f"sensor stale ({snap['sensor_age']:.1f}s) -> setpoint 0")
            self.power_setpoint = 0.0
            return

        err = snap['ovr'] + snap['sensor']
        p = snap['kp'] * err
        integ = self.power_setpoint_integral + snap['ki'] * err * snap['dt']
        u = p + integ

        u_max = snap['power_limit']
        if self.state == State.CHARGE_HEATER_ONLY:
            u_min = -POWER_LIMIT_HEAT
        else:
            u_min = -snap['power_limit_charge'] - POWER_LIMIT_HEAT

        u_clamped = max(u_min, min(u_max, u))
        # Back-calculation: keep integral consistent with the clamped output so
        # we don't wind up while saturated.
        self.power_setpoint_integral = u_clamped - p
        self.power_setpoint = u_clamped
        self._power_limit_charge_snap = snap['power_limit_charge']

    # ---------- State machine ----------
    def _reset_tx_flags(self):
        self.tx_juntek_switch_off = False
        self.tx_juntek_switch_on = False
        self.tx_juntek_charge = False
        self.tx_soyo_discharge = False
        self.tx_soyo_off = False

    def _update_battery_state(self):
        if self.battery_state == BatteryState.INIT:
            return
        if (self.charger_state == 1
                and self.charge_current <= CHARGE_DONE_CURRENT
                and self.voltage >= VOLTAGE_UPPER_LIMIT - CHARGE_FULL_VOLTAGE_MARGIN):
            if self.battery_state != BatteryState.FULL:
                log.info(f"Battery FULL (charger done), {self.voltage:.2f} V")
            self.battery_state = BatteryState.FULL
        elif self.voltage <= VOLTAGE_LOWER_LIMIT + VOLTAGE_HYSTERESIS:
            if self.battery_state != BatteryState.EMPTY:
                log.info(f"Battery EMPTY, {self.voltage:.2f} V")
            self.battery_state = BatteryState.EMPTY

    def _state_machine(self, snap):
        now = snap['now']
        enable = snap['enable']
        self._reset_tx_flags()

        if snap['battery_state_ovr'] is not None:
            self.battery_state = snap['battery_state_ovr']
            log.info(f"battery_state set to {self.battery_state} via MQTT")

        if self.state == State.ERROR:
            log.info("ERROR -> IDLE")
            self.last_start = now
            self.power_setpoint = 0.0
            self.power_setpoint_integral = 0.0
            self.state = State.IDLE
            self.battery_state = BatteryState.INIT
            self.tx_soyo_off = True
            self.tx_juntek_switch_off = True
            return

        if enable == 0 and self.state in (State.CHARGE, State.DISCHARGE,
                                          State.CHARGE_HEATER_ONLY):
            log.debug(f"disable: {self.state} -> ON")
            self.state = State.ON
            self.tx_soyo_off = True
            self.tx_juntek_switch_off = True
            return

        if enable == 0 and self.state == State.ON:
            log.debug("disable: ON -> IDLE")
            self.state = State.IDLE
            return

        if self.state == State.IDLE:
            if enable == 1 and (now - self.last_start).total_seconds() > START_DELAY:
                log.info("IDLE -> ON")
                self.state = State.ON
            return

        self._update_battery_state()

        if self.state == State.ON:
            self._enter_on(snap)
        elif self.state == State.CHARGE:
            self._handle_charge(snap)
        elif self.state == State.CHARGE_HEATER_ONLY:
            self._handle_charge_heater_only(snap)
        elif self.state == State.DISCHARGE:
            self._handle_discharge(snap)

    def _bat_ok_log(self, snap):
        if snap['ovr'] == 0 and self.battery_state != BatteryState.OK:
            log.info(f"Battery OK, {self.voltage:.2f} V")
            self.battery_state = BatteryState.OK

    def _enter_on(self, snap):
        self.tx_juntek_switch_off = True
        if (self.power_setpoint > self.power_eps_discharge
                and self.voltage >= VOLTAGE_LOWER_LIMIT
                and self.battery_state != BatteryState.EMPTY):
            self.power_eps_discharge = POWER_EPS
            self.state = State.DISCHARGE
            self._bat_ok_log(snap)
            log.debug(f"ON -> DISCHARGE @ {self.voltage:.2f} V")
        elif self.power_setpoint < -POWER_EPS:
            if (self.voltage <= VOLTAGE_UPPER_LIMIT
                    and self.battery_state != BatteryState.FULL):
                self.state = State.CHARGE
                self._bat_ok_log(snap)
                log.debug(f"ON -> CHARGE @ {self.voltage:.2f} V")
            else:
                self.state = State.CHARGE_HEATER_ONLY
                log.debug(f"ON -> CHARGE_HEATER_ONLY @ {self.voltage:.2f} V")

    def _handle_charge(self, snap):
        if self.power_setpoint > self.power_eps_discharge:
            self.state = State.ON
            log.debug("CHARGE -> ON (positive setpoint)")
            return
        if (self.power_setpoint < -POWER_EPS
                and self.voltage < VOLTAGE_UPPER_LIMIT
                and self.battery_state != BatteryState.FULL):
            # stay in CHARGE
            pass
        else:
            self.state = State.ON
            log.debug("CHARGE -> ON")
            return

        if self.charger_state == 0:
            self.tx_juntek_switch_on = True
        else:
            self.tx_juntek_charge = True

    def _handle_charge_heater_only(self, snap):
        self.power_eps_discharge = POWER_EPS_HEATER_ONLY
        if self.power_setpoint > self.power_eps_discharge:
            self.state = State.ON
            log.debug("CHARGE_HEATER_ONLY -> ON")
            return
        # heater is driven by DAC at end of cycle; no Modbus writes needed

    def _handle_discharge(self, snap):
        if (self.power_setpoint > self.power_eps_discharge
                and self.voltage > VOLTAGE_LOWER_LIMIT
                and self.battery_state != BatteryState.EMPTY):
            self.tx_soyo_discharge = True
        else:
            self.state = State.ON
            log.debug("DISCHARGE -> ON")

    # ---------- Heater / DAC ----------
    def _compute_heater_voltage(self, snap):
        if self.state == State.CHARGE:
            v = (-self.power_setpoint - snap['power_limit_charge']) \
                / POWER_LIMIT_HEAT * self.dac_vmax
        elif self.state == State.CHARGE_HEATER_ONLY:
            v = (-self.power_setpoint) / POWER_LIMIT_HEAT * self.dac_vmax
        else:
            v = 0.0
        v = max(0.0, min(self.dac_vmax, v))
        if (snap['heater_temp'] > HEATER_TEMP_MAX
                or snap['heater_temp_age'] > HEATER_TEMP_STALE_AFTER):
            if v > 0.0:
                log.info(
                    f"heater cut: temp={snap['heater_temp']:.1f} "
                    f"age={snap['heater_temp_age']:.1f}s"
                )
            v = 0.0
        return v * HEATER_DAC_GAIN

    def _dac_write(self, voltage):
        value = int(voltage / self.dac_vmax * ((1 << 12) - 1))
        self._dac_write_raw(value)

    def _dac_write_raw(self, value):
        value = max(0, min((1 << 12) - 1, int(value)))
        if value == self._last_dac:
            return
        try:
            self.i2c_bus.write_i2c_block_data(
                DAC_ADDR, 0, [0x40, (value >> 8) & 0xff, value & 0xff]
            )
            self._last_dac = value
        except OSError as e:
            log.warning(f"DAC write failed: {e}")

    # ---------- Writes ----------
    def _do_writes(self):
        if self.tx_juntek_switch_off:
            self._send_juntek_with_echo(JUNTEK_SWITCH_OFF, JUNTEK_OFF_ECHO_PFX)
        if self.tx_juntek_switch_on:
            self._send_juntek_with_echo(JUNTEK_SWITCH_ON)
        if self.tx_juntek_charge:
            self._send_juntek_with_echo(
                self._juntek_charge_frame(), JUNTEK_CHRG_FRAME_HDR
            )
        if self.tx_soyo_discharge:
            self._send_soyo(self._soyo_discharge_frame())
        if self.tx_soyo_off:
            self._send_soyo(SOYO_OFF)

    # ---------- MQTT publish (throttled) ----------
    def _publish(self):
        now = datetime.now()
        if (now - self.last_publish).total_seconds() < PUBLISH_PERIOD:
            return
        self.last_publish = now
        t = self.mqtt_topic_root + '/'
        try:
            self.mqtt.publish(t + 'voltage', round(self.voltage, 2))
            self.mqtt.publish(t + 'setpoint', round(self.power_setpoint, 2))
            self.mqtt.publish(t + 'heartbeat', self.heartbeat)
            self.mqtt.publish(t + 'state', self.state.value)
            self.mqtt.publish(t + 'battery_state', self.battery_state.value)
            self.mqtt.publish(t + 'charger_state', self.charger_state)
            self.mqtt.publish(t + 'charge_current', round(self.charge_current, 3))
            heater_w = (self.heater_voltage / self.dac_vmax
                        * POWER_LIMIT_HEAT / 0.9)
            self.mqtt.publish(t + 'heater_voltage', round(heater_w, 2))
        except Exception as e:
            log.warning(f"MQTT publish failed: {e}")

    # ---------- One control cycle ----------
    def _cycle(self):
        self.heartbeat = (self.heartbeat + 1) & 0xff
        now_m = time.monotonic()
        cycle_dt = min(now_m - self._last_cycle_t, 0.5)
        self._last_cycle_t = now_m
        snap = self._snapshot()
        snap['dt'] = cycle_dt
        # 1. read first so PI/state machine see fresh voltage
        self._read_juntek_r01()
        # 2. PI + state transitions
        self._pi_update(snap)
        self._state_machine(snap)
        # 3. transmit any required commands
        self._do_writes()
        # 4. compute & write heater DAC (only writes I2C if value changed)
        self.heater_voltage = self._compute_heater_voltage(snap)
        self._dac_write(self.heater_voltage)
        # 5. publish (throttled)
        self._publish()

    # ---------- Safety / shutdown ----------
    def _send_safe_state(self):
        try:
            self._send_juntek_with_echo(JUNTEK_SWITCH_OFF, JUNTEK_OFF_ECHO_PFX)
        except Exception as e:
            log.info(f"shutdown juntek off failed: {e}")
        try:
            self._send_soyo(SOYO_OFF)
        except Exception as e:
            log.info(f"shutdown soyo off failed: {e}")
        self._dac_write(0.0)

    # ---------- Thread entry ----------
    def run(self):
        log.info("BatteryControl thread starting")
        while not self.stop_signal.is_set():
            if not self.restart_modbus():
                if self.stop_signal.wait(RECONNECT_BACKOFF):
                    break
                continue
            try:
                self._connect()
            except (OSError, socket.timeout) as e:
                log.warning(f"connect failed: {e}")
                self._close()
                if self.stop_signal.wait(RECONNECT_BACKOFF):
                    break
                continue

            # main control loop -------------------------------------------
            try:
                consecutive_timeouts = 0
                while not self.stop_signal.is_set():
                    t0 = time.monotonic()
                    try:
                        self._cycle()
                        consecutive_timeouts = 0
                    except socket.timeout as e:
                        consecutive_timeouts += 1
                        log.debug(
                            f"cycle timeout {consecutive_timeouts}/{MAX_CONSECUTIVE_TIMEOUTS}: {e}"
                        )
                        if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS:
                            log.warning("too many consecutive timeouts; reconnecting")
                            self.state = State.ERROR
                            break
                        # socket still valid; _drain() clears any late response next cycle
                    except (ConnectionError, ValueError, OSError) as e:
                        log.warning(f"cycle error: {e}; reconnecting")
                        self.state = State.ERROR
                        break
                    elapsed = time.monotonic() - t0
                    sleep_for = LOOP_PERIOD - elapsed
                    if sleep_for > 0 and self.stop_signal.wait(sleep_for):
                        break
            except Exception as e:
                log.exception(f"unexpected control-loop exception: {e}")

            # try to leave hardware in a safe state, then reconnect
            try:
                self._send_safe_state()
            except Exception:
                pass
            self._close()

        # clean shutdown
        self._dac_write(0.0)
        log.info("BatteryControl thread stopped")


# ============================================================
# MeterReader: smart-me grid meter -> controller (+ MQTT republish)
# ============================================================
class MeterReader(Thread):
    """Reads the grid meter as fast as it answers and hands Pt straight to the
    controller. MQTT is only used to publish, so control keeps working when
    the broker (on "home") is down."""

    def __init__(self, stop_signal, mqtt_client, get_ctrl,
                 host=METER_HOST, port=METER_PORT, timeout=METER_TIMEOUT):
        super().__init__(name="MeterReader", daemon=True)
        self.stop_signal = stop_signal
        self.mqtt = mqtt_client
        self.get_ctrl = get_ctrl        # callable: ctrl may be re-created by main()
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = None
        self._lock = Lock()
        self._storage = [0.0, 0.0, 0.0]  # per-phase storage power from supla

    def set_storage_phase(self, phase, watts):
        with self._lock:
            self._storage[phase - 1] = float(watts)

    def _connect(self):
        self.sock = socket.create_connection((self.host, self.port), self.timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(self.timeout)
        log.info(f"Meter connected {self.host}:{self.port}")

    def _close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def _recv_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("meter closed connection")
            buf.extend(chunk)
        return bytes(buf)

    def _read(self):
        self.sock.sendall(METER_REQ)
        frame = self._recv_exact(METER_RESP_LEN)
        if frame[0:2] != METER_REQ[0:2] or frame[7] != 0x03 or frame[8] != 80:
            raise ValueError(f"bad meter frame: {frame[:9].hex()}")
        pt, p1, p2, p3 = struct.unpack_from(">iiii", frame, METER_OFF_POWER)
        e_imp, e_exp = struct.unpack_from(">QQ", frame, METER_OFF_ENERGY)
        return pt / 1000.0, p1 / 1000.0, p2 / 1000.0, p3 / 1000.0, \
            e_imp / 1000.0, e_exp / 1000.0

    def _publish(self, pt, p1, p2, p3, e_imp, e_exp):
        with self._lock:
            p_storage = -sum(self._storage) + 15
        t = METER_TOPIC_ROOT + '/'
        try:
            self.mqtt.publish(t + 'energy_import', e_imp)
            self.mqtt.publish(t + 'energy_export', e_exp)
            self.mqtt.publish(t + 'Pt', pt)
            self.mqtt.publish(t + 'P1', p1)
            self.mqtt.publish(t + 'P2', p2)
            self.mqtt.publish(t + 'P3', p3)
            self.mqtt.publish(t + 'Pstorage', p_storage)
        except Exception as e:
            log.debug(f"meter publish failed: {e}")

    def run(self):
        log.info("MeterReader thread starting")
        while not self.stop_signal.is_set():
            try:
                self._connect()
                while not self.stop_signal.is_set():
                    values = self._read()
                    # control input first, publishing must never delay it
                    self.get_ctrl().set_power_sensor(values[0])
                    self._publish(*values)
            except (OSError, ConnectionError, ValueError) as e:
                log.warning(f"meter error: {e}; reconnecting")
            except Exception as e:
                log.exception(f"unexpected meter exception: {e}")
            self._close()
            self.stop_signal.wait(METER_RECONNECT_BACKOFF)
        self._close()
        log.info("MeterReader thread stopped")


# ============================================================
# MQTT callbacks (paho-mqtt v1 callback signature)
# userdata = {'ctrl': BatteryControl, 'meter': MeterReader}
# ============================================================
def on_connect_mqtt(client, ud, flags, rc):
    ctrl = ud['ctrl']
    log.info(f"MQTT connected (rc={rc})")
    client.subscribe("home/heating/watertemp")
    for phase in (1, 2, 3):
        client.subscribe(f"{STORAGE_TOPIC_BASE}/{phase}/power_active")
    for sub in ("enable", "ovr", "kp", "ki", "batmax", "batmin", "batstate"):
        client.subscribe(f"{ctrl.mqtt_topic_root}/{sub}")


def on_disconnect_mqtt(client, userdata, rc):
    log.info(f"MQTT disconnected (rc={rc})")


def on_message_mqtt(client, ud, msg):
    ctrl = ud['ctrl']
    try:
        payload = msg.payload.decode('ascii', errors='replace').strip()
        topic = msg.topic
        if topic.startswith(STORAGE_TOPIC_BASE + "/") and topic.endswith("/power_active"):
            phase = int(topic[len(STORAGE_TOPIC_BASE) + 1:].split('/')[0])
            ud['meter'].set_storage_phase(phase, float(payload))
        elif topic == "home/heating/watertemp":
            ctrl.set_heater_temp(float(payload))
        elif topic == ctrl.mqtt_topic_root + "/enable":
            ctrl.set_enable(int(payload))
        elif topic == ctrl.mqtt_topic_root + "/batmax":
            ctrl.set_power_limit_charge(float(payload))
        elif topic == ctrl.mqtt_topic_root + "/batmin":
            ctrl.set_power_limit(float(payload))
        elif topic == ctrl.mqtt_topic_root + "/ovr":
            ctrl.set_ovr(int(payload))
        elif topic == ctrl.mqtt_topic_root + "/kp":
            ctrl.set_kp(float(payload))
        elif topic == ctrl.mqtt_topic_root + "/ki":
            ctrl.set_ki(float(payload))
        elif topic == ctrl.mqtt_topic_root + "/batstate":
            ctrl.set_battery_state(int(payload))
    except (ValueError, AttributeError) as e:
        log.warning(f"bad mqtt payload on {msg.topic}: {e}")


# ============================================================
# main
# ============================================================
def _make_mqtt_client():
    # Compatible with both paho-mqtt 1.x and 2.x
    try:
        return paho.Client(paho.CallbackAPIVersion.VERSION1, MQTT_CLIENT_ID)
    except (AttributeError, TypeError):
        return paho.Client(MQTT_CLIENT_ID)


def main():
    log.info("init")
    stop_signal = Event()
    install_signal_handlers(stop_signal)

    mqtt = _make_mqtt_client()
    mqtt.on_connect = on_connect_mqtt
    mqtt.on_message = on_message_mqtt
    mqtt.on_disconnect = on_disconnect_mqtt
    mqtt.reconnect_delay_set(min_delay=1, max_delay=30)
    try:
        mqtt.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    except OSError as e:
        log.warning(f"MQTT connect failed: {e}; will reconnect in background")

    ud = {}
    ud['ctrl'] = BatteryControl(stop_signal, MODBUS_HOST, MODBUS_PORT, mqtt)
    ud['meter'] = MeterReader(stop_signal, mqtt, lambda: ud['ctrl'])
    mqtt.user_data_set(ud)
    mqtt.loop_start()
    ud['ctrl'].start()
    ud['meter'].start()

    while not stop_signal.wait(2.0):
        if not ud['ctrl'].is_alive():
            log.warning("control thread died, restarting")
            ud['ctrl'] = BatteryControl(stop_signal, MODBUS_HOST, MODBUS_PORT, mqtt)
            ud['ctrl'].start()
        if not ud['meter'].is_alive():
            log.warning("meter thread died, restarting")
            ud['meter'] = MeterReader(stop_signal, mqtt, lambda: ud['ctrl'])
            ud['meter'].start()

    log.info("Shutting down")
    ud['meter'].join(5)
    ud['ctrl'].join(15)
    mqtt.disconnect()
    mqtt.loop_stop()
    log.info("End")


if __name__ == "__main__":
    main()
