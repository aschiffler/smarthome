from datetime import datetime
from json import loads, dumps
import socket, time, signal
from threading import Thread, Event, currentThread
import paho.mqtt.client as paho
import logging, sys
import numpy as np
from systemd import journal
import requests
from requests.auth import HTTPBasicAuth
from smbus2 import SMBus

# singal processing
from collections import deque
import numpy as np

class LiveFilter:
    def process(self, x):
        if np.isnan(x):
            return x
        return self._process(x)

    def __call__(self, x):
        return self.process(x)

    def _process(self, x):
        raise NotImplementedError("Derived class must implement _process")

class LiveLFilter(LiveFilter):
    def __init__(self, 
                b=np.array([0.43284664499029185, 1.7313865799611674,  2.597079869941751, 1.7313865799611674,  0.43284664499029185]),
                a=np.array([1.0,                 2.369513007182037,   2.313988414415879, 1.0546654058785672,  0.18737949236818482])):
#                b=np.array([0.09398085143379446, 0.37592340573517785, 0.5638851086027667, 0.37592340573517785, 0.09398085143379446]),
#                a=np.array([1.0000000000000000e+00, -1.3877787807814457e-17,  4.8602882206826953e-01 ,-6.1959145060642913e-18,  1.7664800872441891e-02])):
        # digital design 0.4 * wn, 4th order
        self.b = b
        self.a = a
        self._xs = deque([0] * len(b), maxlen=len(b))
        self._ys = deque([0] * (len(a) - 1), maxlen=len(a)-1)
        
    def _process(self, x):
        self._xs.appendleft(x)
        y = np.dot(self.b, self._xs) - np.dot(self.a[1:], self._ys)
        y = y / self.a[0]
        self._ys.appendleft(y)
        return y 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        #logging.StreamHandler(),
        journal.JournaldLogHandler()
    ]
)


class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)
    return

  def exit_gracefully(self, *args):
    self.kill_now = True
    return

def calc_crc(data):
    crc = 0xFFFF
    for pos in data:
        crc ^= pos
        for i in range(8):
            if ((crc & 1) != 0):
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    return crc    

from enum import Enum
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

class BatteryControl(Thread):
    def __init__(
            self,
            signal,
            host,
            port,
            mqtt_topic_root,
            timeout=1,
            on_connected_callback=None,
            on_receive_callback=None,
            on_disconnected_callback=None,
            userdata=None,
            ):

        self.host = host
        self.port = port
        self.timeout = timeout
        self.on_connected_callback = on_connected_callback
        self.on_receive_callback = on_receive_callback
        self.on_disconnected_callback = on_disconnected_callback
        self.stop_signal = signal
        self.request = bytearray()
        self.mqtt_topic_root = mqtt_topic_root
        self.dictionary = dict()
        #self.dictionary[1]   = {'dir':'read','nm': 'soyo', 'rq': bytes([0x24,0x55,0x00,0x21,0x00,0x00,0x80,0x60]), 'recv':bytes([0x01])}
        self.dictionary[2]   = {'dir':'read','nm':'juntek_r01','rq': bytes([0x02, 0x03, 0x10, 0x00, 0x00, 0x03, 0x01, 0x38,0x00]), 'recv':bytes([0x02,0x03,0x06])}
        #
        self.write_dictionary = dict()
        self.write_dictionary[3] = {'en':False,'nm': 'juntek_switch_off',   'rq': bytes([0x02, 0x06, 0x00, 0x02, 0x00, 0x00, 0x28, 0x39]), 'recv':bytes([0x02, 0x06, 0x00, 0x02]),  'err': bytes([0x02, 0x06, 0x00, 0x02, 0x00, 0x00, 0x28, 0x39])}
        self.write_dictionary[4] = {'en':False,'nm': 'juntek_switch_on',    'rq': bytes([0x02, 0x06, 0x00, 0x02, 0x00, 0x01, 0xe9, 0xf9]), 'recv':bytes([0x66]),                    'err': bytes([0x02, 0x06, 0x00, 0x02, 0x00, 0x00, 0x28, 0x39])}
        self.write_dictionary[5] = {'en':False,'nm': 'juntek_charge',       'rq': bytes([0x02, 0x06, 0x00, 0x01, 0x00, 0x00, 0xd8, 0x39]), 'recv':bytes([0x66]),                    'err': bytes([0x02, 0x06, 0x00, 0x02, 0x00, 0x00, 0x28, 0x39])}
        self.write_dictionary[6] = {'en':False,'nm': 'soyo_discharge',      'rq': bytes([0x24,0x56,0x00,0x21,0x00,0x28,0x80,0xe0]),        'recv':bytes([0x00]),                    'err': bytes([0x24,0x56,0x00,0x21,0x00,0x00,0x80,0x08])}
        self.write_dictionary[7] = {'en':False,'nm': 'soyo_off',            'rq': bytes([0x24,0x56,0x00,0x21,0x00,0x00,0x80,0x08]),        'recv':bytes([0x00]),                    'err': bytes([0x24,0x56,0x00,0x21,0x00,0x00,0x80,0x08])}
        #
        self.mqtt = userdata
        self.power_sensor = 0.0
        self.power_setpoint = 0.0
        self.power_setpoint_integral = 0.0
        Thread.__init__(self)
        # signal processing
        self.input_filter = LiveLFilter()
        self.input_filter_voltage = LiveLFilter()
        self.charger_state = 0
        self.voltage = 26.0
        self.charge_current = 0.0
        self.heartbeat = 0
        self.enable = 0
        self.state = State.IDLE
        self.battery_state = BatteryState.INIT
        self.power_limit = 700.0
        self.power_limit_charge = 1200.0
        self.power_limit_heat = 1300.0
        self.power_heat_min = 90.0
        self.power_heat_min_count = 0
        self.voltage_lower_limit = -1.0
        self.voltage_hysteresis = 1.8
        self.voltage_upper_limit = 28.6
        self.power_eps = 15.0
        self.power_eps_discharge = 15.0
        self.power_sensor_last_set = datetime.now()
        self.heater_last_set = datetime.now()
        self.heater_temp_last_set = datetime.now()
        self.heater_temp = 0.0
        self.last_start = datetime.now()
        self.ovr = 0
        self.heater_voltage_steps = [0,0.25,0.5,0.75,1.0,1.25,1.5,1.75,2.0,2.25,2.5,2.75,3.0,3.25,3.5,3.75,4.0,4.25,4.5,4.75,5.0,5.25,5.5,5.75,6.0,6.25,6.5,6.75,7.0,7.25,7.5,7.75,8.0,8.25,8.5,8.75,9.0,9.25,9.5,9.75,10.0] 
        self.heater_voltage_act_step = 0
        self.kp = 0.6
        self.ki = 0.03
        self.auth_modbus = HTTPBasicAuth('admin', 'admin')
        self.dac_vmax = 4.55
        self.i2c_bus = SMBus(3)
        self.i2c_bus.write_i2c_block_data(0x60, 0, [0x40,0,0])

    def restartModbus(self):
        try:
            requests.get('http://' + self.host + '/login.cgi?restart',auth=self.auth_modbus)
            logging.info('Restart Modbus...')
        except:
            logging.info('Restart Modbus failed')
            return False
        time.sleep(10)
        logging.info('Modbus restarted')
        self.last_start = datetime.now()
        return True

    # run by the Thread object
    def run(self):
        logging.info('Starting battery...')
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        if not self.restartModbus():
            return
        # self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            logging.debug('Connecting...')
            self.sock.connect((self.host, self.port))
            if self.on_connected_callback:
                self.on_connected_callback(self.host, self.port)
        except Exception as e:
            logging.info(f'Timeout during connection {e}')
            self.sock.close()
            return False
        # start a thread to read on socket
        controlloop = Thread(
            target=self.controlloop,
            args=(self.on_receive_callback, self.on_disconnected_callback)
        )
        controlloop.start()
        while controlloop.is_alive():
            if not controlloop.is_alive():
                logging.info('controlloop not alive')
                break # restart
        controlloop.join(15)
        try:
            self.sock.close()
            self.on_disconnected_callback()
        except:
            logging.info('tried to close socket but was already closed')
        return

    def read_request(self,addr,name):
        return
    
    def update_write_registers(self):
        for _,item in self.write_dictionary.items():
            if item.get('nm') == 'soyo_discharge':
                if self.voltage != 0.0:
                    hb = (int(self.power_setpoint) >> 8) & 0xff ## AC set point for soyo
                    lb = (int(self.power_setpoint) >> 0) & 0xff
                else:
                    hb = 0x00
                    lb = 0x00
                frame = bytearray([0x24,0x56,0x00,0x21,hb,lb,0x80,(264-hb-lb)& 0xff])
                item['rq'] = frame
            if item.get('nm') == 'juntek_charge':
                if self.voltage != 0.0:
                    current = -self.power_setpoint / self.voltage / 0.95 # 95% efficiency AC-> DC
                else:
                    #if self.battery_state != BatteryState.EMPTY:
                     #   logging.info('Battery Voltage is Zero!')   
                        #self.battery_state = BatteryState.EMPTY
                    # Try as small current if BMS is in state of dishcharege protection
                    current = 0.5
                # limit
                if current * self.voltage > self.power_limit_charge and self.voltage > 0.0:
                    current = self.power_limit_charge / self.voltage
                if current > 40.0:  
                    current = 40.0
                if current < 0.0:
                    current = 0.0
                logging.debug(f'Charge Current {round(current,2)}')                    
                frame = bytearray([0x02, 0x06, 0x00, 0x01])
                frame.append( (int(current * 1000) >> 8) & 0xff)
                frame.append( (int(current * 1000) >> 0) & 0xff)
                crc=calc_crc(frame)
                frame.append( crc & 0xff)
                frame.append((crc >> 8) & 0xff)
                item['rq'] = frame
                #
        heater_voltage = 0.0
        if self.state == State.CHARGE:
            heater_voltage = (-self.power_setpoint - self.power_limit_charge ) / self.power_limit_heat * self.dac_vmax
        if self.state == State.CHARGE_HEATER_ONLY:
            heater_voltage = (-self.power_setpoint  )  / self.power_limit_heat * self.dac_vmax
        if self.state == State.ON or self.state == State.DISCHARGE or self.state == State.IDLE or self.state == State.ERROR:
            heater_voltage = 0.0
        if heater_voltage < 0.0:
            heater_voltage = 0.0
        if heater_voltage > self.dac_vmax:
            heater_voltage = self.dac_vmax
        if self.heater_temp > 70.0 or (datetime.now() - self.heater_temp_last_set).total_seconds() > 20.0:
            heater_voltage = 0.0
        heater_voltage = heater_voltage * 0.85

        value = int(heater_voltage/self.dac_vmax * ((1<<12)-1) )
        
        #if (datetime.now()-self.heater_last_set).total_seconds() > 0.5:
        #    self.heater_last_set = datetime.now()
        try:
            self.i2c_bus.write_i2c_block_data(0x60, 0, [0x40, (value >> 8) & 0xff,value & 0xFF])
        except Exception as e:
            logging.info(f'dac failed: {e}')
        self.heater_voltage = heater_voltage
        return

    def controler(self):
        if self.enable == 1:
            control = ( self.ovr + self.power_sensor  )
            self.power_setpoint = self.kp * control
            #
            self.power_setpoint_integral += self.ki * control
            if self.power_setpoint_integral > self.power_limit:
                self.power_setpoint_integral = self.power_limit
                logging.debug('Integral Limit up')
            if self.power_setpoint_integral < -self.power_limit_charge - self.power_limit_heat and not self.state == State.CHARGE_HEATER_ONLY:
                self.power_setpoint_integral = -self.power_limit_charge -self.power_limit_heat
                logging.debug('Integral Limit down')
            if self.power_setpoint_integral < - self.power_limit_heat and self.state == State.CHARGE_HEATER_ONLY:
                self.power_setpoint_integral = -self.power_limit_heat
                logging.debug('Integral Limit down')
            #
            self.power_setpoint += self.power_setpoint_integral
            # 
            if self.power_setpoint > self.power_limit:
                self.power_setpoint = self.power_limit
            if self.power_setpoint < -self.power_limit_charge - self.power_limit_heat and not self.state == State.CHARGE_HEATER_ONLY:
                self.power_setpoint = -self.power_limit_charge - self.power_limit_heat
            if self.power_setpoint < - self.power_limit_heat and self.state == State.CHARGE_HEATER_ONLY:                
                self.power_setpoint = - self.power_limit_heat
            rtt = (datetime.now()-self.power_sensor_last_set).total_seconds()
            logging.debug(f'rtt: {rtt} [s]')
            if rtt > 5.0:
                logging.info(f'do not receive external sensor value. rtt was: {rtt} [s]')
                self.power_setpoint = 0.0
        else:
            self.power_setpoint = 0.0
        logging.debug(f'Set Point [W]: {int(self.power_setpoint)}')
        ## state machine
        if self.enable == 1 and self.state == State.IDLE:
            logging.info(f'Enable 1 wait... for State IDLE -> ON')
            self.power_setpoint = 0.0
            time.sleep(0.5)
        # switch on
        if self.enable == 1 and self.state == State.IDLE and (datetime.now()-self.last_start).total_seconds() > 2.0:
            logging.info(f'Enable 1 State IDLE -> ON')
            self.state = State.ON
        if self.enable == 0 and (self.state == State.CHARGE or self.state == State.DISCHARGE or self.state == State.ERROR or self.state == State.CHARGE_HEATER_ONLY):
            logging.debug(f'Enable 0 State {self.state} -> ON')
            self.state = State.ON
            for _,item in self.write_dictionary.items():
                if item.get('nm') == 'soyo_off':
                    item['en'] = True
                if item.get('nm') == 'soyo_discharge':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_on':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_off':
                    item['en'] = True
            logging.debug(f'{self.state}')
            self.update_write_registers()
            return
        # switch off
        if self.enable == 0 and self.state == State.ON:
            logging.debug(f'Enable 0 State ON -> IDLE')
            self.state = State.IDLE
            for _,item in self.write_dictionary.items():
                if item.get('nm') == 'soyo_off':
                    item['en'] = False
                if item.get('nm') == 'soyo_discharge':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_on':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_off':
                    item['en'] = False
            logging.debug(f'{self.state}')
            self.update_write_registers()            
            return
        ##------------ERROR----------------
        if self.state == State.ERROR:
            logging.info(f'Reset State ERROR -> IDLE')
            self.last_start = datetime.now()
            self.power_setpoint = 0.0
            self.enable = 1 # keep enabled, disabled is only from external
            self.state = State.IDLE
            self.battery_state = BatteryState.INIT

            for _,item in self.write_dictionary.items():
                if item.get('nm') == 'soyo_off':
                    item['en'] = True
                if item.get('nm') == 'soyo_discharge':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_on':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_off':
                    item['en'] = True
            logging.debug(f'{self.state}')
            self.update_write_registers()
            return      
        ##----------IDLE-------------
        if self.state == State.IDLE:
            logging.debug(f'In IDLE')
            for _,item in self.write_dictionary.items():
                if item.get('nm') == 'soyo_off':
                    item['en'] = False
                if item.get('nm') == 'soyo_discharge':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_on':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_off':
                    item['en'] = False
            logging.debug(f'{self.state}')
            self.update_write_registers()
            return
        ##-----------ON-----------
        if self.state == State.ON:
            logging.debug(f'In ON')
            if self.power_setpoint > self.power_eps_discharge and self.voltage >= self.voltage_lower_limit and not self.battery_state == BatteryState.EMPTY:
                self.power_eps_discharge = self.power_eps
                self.state = State.DISCHARGE
                logging.debug(f'State ON -> DISCHARGE, {self.voltage} [V]')
                if self.ovr == 0:                 
                    if self.battery_state != BatteryState.OK:
                        logging.info(f'Battery OK, {self.voltage} [V]')
                    self.battery_state = BatteryState.OK
            elif self.power_setpoint < -self.power_eps:
                if  self.voltage <= self.voltage_upper_limit and not self.battery_state == BatteryState.FULL:
                    self.state = State.CHARGE
                    if self.ovr == 0:                    
                        if self.battery_state != BatteryState.OK:
                            logging.info(f'Battery OK, {self.voltage} [V]')
                        self.battery_state = BatteryState.OK
                    logging.debug(f'State ON -> CHARGE, {self.voltage} [V]')
                else:
                    self.state = State.CHARGE_HEATER_ONLY
                    logging.debug(f'State ON -> CHARGE_HEATER_ONLY, {self.voltage} [V]')
            elif self.power_setpoint <= self.power_eps and self.power_setpoint >= -self.power_eps:
                self.state = State.ON # stay here change nothing
            if self.charger_state == 1 and self.charge_current <= 2.0 and self.battery_state != BatteryState.INIT:
                if self.battery_state != BatteryState.FULL:
                    logging.info(f'Battery FULL bc. Charger State, {self.voltage} [V]')
                self.battery_state = BatteryState.FULL
            elif self.voltage <= self.voltage_lower_limit + self.voltage_hysteresis and self.battery_state != BatteryState.INIT:
                if self.battery_state != BatteryState.EMPTY:
                    logging.info(f'Battery EMPTY, {self.voltage} [V]')
                self.battery_state = BatteryState.EMPTY
            # always 
            for _,item in self.write_dictionary.items():
                if item.get('nm') == 'soyo_off':
                    item['en'] = False # soyo off is done by not sending any request
                if item.get('nm') == 'soyo_discharge':
                    item['en'] = False
                if item.get('nm') == 'juntek_charge':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_on':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_off':
                    item['en'] = True
            logging.debug(f'{self.state}')
            self.update_write_registers()
            return                        
        ##-----------CHARGE-----------
        if self.state == State.CHARGE:
            logging.debug(f'In CHARGE')
            if self.power_setpoint > self.power_eps_discharge: 
                self.state = State.ON  # CHARGE -> DISCHARGE: go always other state ON
                logging.debug(f'State CHARGE -> ON')
                return # do not change registers
            elif self.power_setpoint < -self.power_eps and self.voltage < self.voltage_upper_limit and not self.battery_state == BatteryState.FULL:
                self.state = State.CHARGE  # stay here
            else:
                self.state = State.ON
                logging.debug(f'State CHARGE -> ON')
            if self.charger_state == 0: # charger is off -> switch on
                for _,item in self.write_dictionary.items():
                    if item.get('nm') == 'soyo_off':
                        item['en'] = False
                    if item.get('nm') == 'soyo_discharge':
                        item['en'] = False
                    if item.get('nm') == 'juntek_switch_on':
                        item['en'] = True
                    if item.get('nm') == 'juntek_switch_off':
                        item['en'] = False
            if self.charger_state > 0: # charger is on -> set current
                for _,item in self.write_dictionary.items():
                    if item.get('nm') == 'juntek_switch_on':
                        item['en'] = False
                    if item.get('nm') == 'juntek_charge':
                        item['en'] = True
            if self.charger_state == 1 and self.charge_current <= 2.0 and self.battery_state != BatteryState.INIT:
                if self.battery_state != BatteryState.FULL:
                    logging.info(f'Battery FULL, {self.voltage} [V]')
                self.battery_state = BatteryState.FULL
            logging.debug(f'{self.state}')
            self.update_write_registers()
            return                       
        ##-----------CHARGE_HEATER_ONLY-----------
        if self.state == State.CHARGE_HEATER_ONLY:
            self.power_eps_discharge = 50.0 # Allow some overcharge
            logging.debug(f'In CHARGE_HEATER_ONLY')
            if self.power_setpoint > self.power_eps_discharge: 
                self.state = State.ON  # CHARGE_HEATER_ONLY -> DISCHARGE: go always other state ON
                logging.debug(f'State CHARGE_HEATER_ONLY -> ON')
                return # do not change registers
            elif self.power_setpoint < -self.power_eps:
                self.state = State.CHARGE_HEATER_ONLY  # stay here
            for _,item in self.write_dictionary.items():
                if item.get('nm') == 'juntek_switch_on':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_off':
                    item['en'] = False
                if item.get('nm') == 'juntek_charge':
                    item['en'] = False
            logging.debug(f'{self.state}')
            self.update_write_registers()
            return                                    
        ##-----------DISCHARGE-----------
        if self.state == State.DISCHARGE:
            logging.debug(f'In DISCHARGE')
            if self.power_setpoint > self.power_eps_discharge and self.voltage > self.voltage_lower_limit and not self.battery_state == BatteryState.EMPTY: 
                self.state = State.DISCHARGE  # stay here
                self.heater_voltage_act_step = 0
            elif self.power_setpoint < -self.power_eps:
                self.state = State.ON  # DISCHARGE -> CHARGE: go always other state ON
                logging.debug(f'State DISCHARGE -> ON')
                return
            else:
                self.state = State.ON
                logging.debug(f'State DISCHARGE -> ON')
            for _,item in self.write_dictionary.items():
                if item.get('nm') == 'juntek_switch_on':
                    item['en'] = False
                if item.get('nm') == 'juntek_switch_off':
                    item['en'] = False                
                if item.get('nm') == 'soyo_off':
                    item['en'] = False
                if item.get('nm') == 'soyo_discharge':
                    item['en'] = True
                if item.get('nm') == 'heiz_off':
                    item['en'] = False # True
                if item.get('nm') == 'heiz_on':
                    item['en'] = False
            logging.debug(f'{self.state}')
            self.update_write_registers()
            return                        
        #------------------------------------------
        logging.info(f'Unhandled State!')
        
        

    def controlloop(self, on_receive_callback, on_disconnected_callback):
        t = currentThread()
        # set a buffer size ( could be 2048 or 4096 / power of 2 )
        size = 32
        self.sock.settimeout(self.timeout)
        loop = True
        write = True
        while loop:
            time.sleep(0.05)
            self.heartbeat = (self.heartbeat + 1) & 0xff
            # IT1 control
            self.controler()
            # Communication 
            # loop over feedback values begin
            for _,item in self.dictionary.items():
                try:
                    self.sock.send(item.get('rq'))
                    logging.debug(f'{item.get("dir")} | {item.get("nm")} | {item.get("rq").hex()}')
                    time.sleep(0.05)
                except:
                    logging.info('send failed for reading set State ERROR')
                    self.state = State.ERROR
                    raise Exception('CLIENT Disconnected send failed')
                if item.get("recv")[0] != 0x00:
                    try:
                        data = self.sock.recv(size)
                        if data:
                            if on_receive_callback:
                                try:
                                    on_receive_callback(self,item,data)
                                except Exception as e:
                                    logging.info(f'Receive Callback Failed: {e} | Set state ERROR')
                                    self.state = State.ERROR
                                    write = False # but loop keep running
                                    # pass # it seems that after a receive failure we shall reconnect
                                    raise Exception('Try to reconnect due to failure in receive callback for reading')
                        else:
                            self.state = State.ERROR
                            raise Exception('No data from socket')
                    except socket.timeout:
                            logging.info(f'Timeout reading: {item.get("nm")} set State ERROR')
                            self.state = State.ERROR
                            write = False
                            raise Exception('Try to reconnect due to timeout during reading') # better timeout counter and then reconnect
                    except Exception as e:
                        logging.info(f'controlloop/reading exception: {e}')
                        self.state = State.ERROR
                        self.sock.close()
                        if on_disconnected_callback:
                            try:
                                on_disconnected_callback(e)
                            except Exception as e:
                                logging.info(f'on_close_callback failed {e}')
                        break
            # loop over feedback values end
            #
            # loop over write values begin only if read was successful
            if write:
                # loop over write values begin
                for _,item in self.write_dictionary.items():
                    if item.get('en'):
                        try:
                            self.sock.send(item.get('rq'))
                            time.sleep(0.05)
                        except:
                            logging.info('send failed for writing set State ERROR')
                            self.state = State.ERROR
                            raise Exception('CLIENT Disconnected send failed')
                        if item.get('recv')[0] != 0x00:
                            try:
                                data = self.sock.recv(size)
                                if data:
                                    if on_receive_callback:
                                        try:
                                            on_receive_callback(self,item,data)
                                        except Exception as e:
                                            logging.info(f'Receive Callback for write Failed: {e} | Set State ERROR')
                                            self.state = State.ERROR
                                            # pass # it seems that after a receive failure we shall reconnect
                                            raise Exception('Try to reconnect due to failure in receive callback for write')
                                            
                                else:
                                    raise ValueError('CLIENT Disconnected')
                            except socket.timeout:
                                logging.info(f'Timeout Write no ACK set State ERROR; Item was: {item.get("nm")}')
                                self.state = State.ERROR
                                # retry
                                pass
                            except Exception as e:
                                logging.info(f'control loop / writing exception: {e} raise exception')      
                                raise Exception('Unhandled')
                                break
                # loop over write values end
            # check if stop signal is set
            if self.stop_signal.wait(0):
                loop = False
                self.power_setpoint = 0.0
                try:
                    for _,item in self.write_dictionary.items():
                        logging.debug(f'Finally try to send {item.get("nm")} | {item.get("err").hex()} set State IDLE')
                        self.sock.send(item.get('err')) # send "zero/init" requests
                        if item.get('recv')[0] != 0x00:
                            data = self.sock.recv(size)
                            logging.info(f'Received {data.hex()}')
                        self.state = State.IDLE
                except:
                    logging.info(f'failed to send final requests for {item.get("nm")}')
                self.sock.close()
        logging.info('control loop stopped')
        return
    
def receive_callback(mybatterycontrol,item,data):
    logging.debug(f'Received {data.hex()}')
    if item.get('recv')[0] == 0x66:	# no check needed
        logging.debug(f'Received data but no check needed')
        return
    if len(data) <= len(item.get('recv')):
        mybatterycontrol.state = State.ERROR
        raise Exception("Error in response; Wrong length ", item.get('nm'))
    for i,b in enumerate(item.get('recv')):
        if b != data[i]:
            # mybatterycontrol.state = State.ERROR
            # raise Exception(f'Error in response; Wrong answer from {item.get("nm")} , Received {data.hex()}')
            logging.debug(f'Error in response; Wrong answer from {item.get("nm")} , Received {data.hex()}')
            return
    #-------Parser-for specific cases--------
    if item.get('nm') == 'juntek_r01':
        mybatterycontrol.charger_state = data[4]
        voltage = (0x100*data[5] + data[6])/100.0
        mybatterycontrol.voltage = voltage
        mybatterycontrol.charge_current = (0x100*data[7] + data[8])/1000.0
    logging.debug(f'Voltage: {mybatterycontrol.voltage} | Charge-Current: {mybatterycontrol.charge_current} | Charger-State: {mybatterycontrol.charger_state}')
    # publish internal states/values on mqtt topic
    try:
        publish_callback(mybatterycontrol)
    except Exception as e:
        mybatterycontrol.state = State.ERROR
        raise Exception("Publish to mqtt topics failed")
    return

def publish_callback(mybatterycontrol):
    topic = mybatterycontrol.mqtt_topic_root + '/'
    mybatterycontrol.mqtt.publish(topic + 'voltage',round(mybatterycontrol.voltage,2))
    mybatterycontrol.mqtt.publish(topic + 'setpoint',round(mybatterycontrol.power_setpoint,2))
    mybatterycontrol.mqtt.publish(topic + 'heartbeat',mybatterycontrol.heartbeat)
    mybatterycontrol.mqtt.publish(topic + 'state',mybatterycontrol.state.value)
    mybatterycontrol.mqtt.publish(topic + 'battery_state',mybatterycontrol.battery_state.value)
    mybatterycontrol.mqtt.publish(topic + 'charger_state',mybatterycontrol.charger_state)
    mybatterycontrol.mqtt.publish(topic + 'charge_current',round(mybatterycontrol.charge_current,3))
    #mybatterycontrol.mqtt.publish(topic + 'heater_voltage',round(mybatterycontrol.heater_voltage_steps[mybatterycontrol.heater_voltage_act_step]*150.0,2))
    mybatterycontrol.mqtt.publish(topic + 'heater_voltage',round(mybatterycontrol.heater_voltage/mybatterycontrol.dac_vmax*mybatterycontrol.power_limit_heat/0.9,2))
    return

def connected_callback(host,port):
    logging.info(f'mybatterycontrol connected: {host}:{port}')
    return

def disconnected_callback(msg=None):
    logging.info(f'mybatterycontrol disconnected / socket closed | {msg}')
    return

def on_connect_mqtt(client, mybatterycontrol, flags, rc):
    logging.info(f'Mqtt Client connected {rc}')
    client.subscribe("home/smartme/Pt")
    client.subscribe(mybatterycontrol.mqtt_topic_root+"/enable")
    client.subscribe("home/heating/watertemp")
    client.subscribe(mybatterycontrol.mqtt_topic_root+"/ovr")
    client.subscribe(mybatterycontrol.mqtt_topic_root+"/kp")
    client.subscribe(mybatterycontrol.mqtt_topic_root+"/ki")
    client.subscribe(mybatterycontrol.mqtt_topic_root+"/batmax")
    logging.debug(f'Subscribed on: home/smartme/Pt')
    logging.debug(f'Subscribed on: {mybatterycontrol.mqtt_topic_root}/enable')
    logging.debug(f'Subscribed on: {mybatterycontrol.mqtt_topic_root}/ovr')
    return

def on_disconnect_mqtt(client, userdata, rc):
    logging.info(f'Mqtt Client disconnected {rc}')
    return

def on_message_mqtt(client, mybatterycontrol, msg):
    #logging.debug(f'{msg.topic} {msg.payload}')
    if msg.topic == "home/smartme/Pt":
        #mybatterycontrol.power_sensor = mybatterycontrol.input_filter(float(msg.payload)) + 5.0
        mybatterycontrol.power_sensor = float(msg.payload) + 5.0
        if mybatterycontrol.state == State.CHARGE or mybatterycontrol.state == State.CHARGE_HEATER_ONLY:
            mybatterycontrol.power_sensor += 25.0
        mybatterycontrol.power_sensor_last_set = datetime.now()
        logging.debug(f'Power Sensor [W]: {int(mybatterycontrol.power_sensor)}')
    if msg.topic == "home/heating/watertemp":
        mybatterycontrol.heater_temp = float(msg.payload)
        mybatterycontrol.heater_temp_last_set = datetime.now()
    if msg.topic == mybatterycontrol.mqtt_topic_root+"/enable":
        mybatterycontrol.enable = int(msg.payload)
    if msg.topic == mybatterycontrol.mqtt_topic_root+"/batmax":
        limit = float(msg.payload)
        if limit > 1200:
            limit = 1200
        if limit < 100:
            limit = 100
        mybatterycontrol.power_limit_charge = limit
    if msg.topic == mybatterycontrol.mqtt_topic_root+"/ovr":
        mybatterycontrol.ovr = int(msg.payload)
    if msg.topic == mybatterycontrol.mqtt_topic_root+"/kp":
        mybatterycontrol.kp = float(msg.payload)
        logging.info(f'kp: {mybatterycontrol.kp}')
    if msg.topic == mybatterycontrol.mqtt_topic_root+"/ki":
        mybatterycontrol.ki = float(msg.payload)
        logging.info(f'ki: {mybatterycontrol.ki}')
    if msg.topic == mybatterycontrol.mqtt_topic_root+"/ovr_bat":
        if int(msg.payload) == 1:
            mybatterycontrol.battery_state = BatteryState.FULL
        if int(msg.payload) == 2:
            mybatterycontrol.battery_state = BatteryState.OK
        if int(msg.payload) > 2:
            mybatterycontrol.heater_voltage_act_step = int(msg.payload)
        if int(msg.payload) > 2:
            logging.info(f'Bat_State: {(mybatterycontrol.heater_voltage_act_step-300)/100.0}')
        else:
            logging.info(f'Bat_State: {mybatterycontrol.battery_state}')
    
    #logging.debug(f'Power Sensor [W]: {int(mybatterycontrol.power_sensor)}')
    return

def main():
    logging.info('init')
    mqtt = paho.Client()
    mqtt.on_connect = on_connect_mqtt
    mqtt.on_message = on_message_mqtt
    mqtt.on_disconnect = on_disconnect_mqtt
    mqtt.connect("192.168.178.6", 1883, 60)
    logging.info('Mqtt Client started')
    stop_signal = Event()
    mybatterycontrol = BatteryControl(
        stop_signal,
        '192.168.0.151',
        8888,
        mqtt_topic_root='home/battery',
        timeout=2,
        on_connected_callback=connected_callback,
        on_receive_callback=receive_callback,
        on_disconnected_callback=disconnected_callback,
        userdata=mqtt,
    )
    mqtt.user_data_set(mybatterycontrol)
    mqtt.loop_start()
    killer = GracefulKiller()
    mybatterycontrol.start()
    mybatterycontrol.join(1)
    while not killer.kill_now:
        if not mybatterycontrol.is_alive():
            logging.info('Try to restart...')
            mybatterycontrol.run()
        time.sleep(2)
    stop_signal.set()
    mqtt.disconnect()
    mqtt.loop_stop()
    mybatterycontrol.join(10)
    logging.info("End of mybatterycontrol.")
    return

if __name__ == "__main__":
    main()
