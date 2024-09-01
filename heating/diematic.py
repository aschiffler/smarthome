from datetime import datetime
from pysolar.solar import *
import pytz, math
import sys
import numpy as np
import requests
from requests.auth import HTTPBasicAuth
from json import loads, dumps
import socket, time, signal
from threading import Thread, Event
import queue
import paho.mqtt.client as paho
import logging
from systemd.journal import JournaldLogHandler

journald_handler = JournaldLogHandler()
journald_handler.setFormatter(logging.Formatter(
    '[%(levelname)s] %(message)s'
))
logger = logging.getLogger('diematic')
logger.addHandler(journald_handler)
logger.setLevel(logging.INFO)



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

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)
    return

  def exit_gracefully(self, *args):
    self.kill_now = True
    return

class DiematicConnect(Thread):
    READ_ANALOG_HOLDING_REGISTERS=0x03;
    WRITE_MULTIPLE_REGISTERS=0x10;
    ANSWER_FRAME_MIN_LENGTH=0x07;
    ANSWER_FRAME_MAX_LENGTH=0x100;
    ADDRESS = 0xa;
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

        self.heartbeat = 0
        self.init_modbus = False
        self.host = host
        self.port = port
        self.timeout = timeout
        self.on_connected_callback = on_connected_callback
        self.on_receive_callback = on_receive_callback
        self.on_disconnected_callback = on_disconnected_callback
        self.stop_signal = signal
        self.read_request_queue = queue.Queue()
        self.write_request_queue = queue.Queue(maxsize=1)
        self.request = bytearray()
        self.mqtt_topic_root = mqtt_topic_root
        self.dictionary = dict()
        self.dictionary[0] = {"adr":7,"nb":14}
        self.dictionary[1] = {"adr":89,"nb":8}
        self.dictionary[2] = {"adr":59,"nb":4}
        self.dictionary[3] = {"adr":427,"nb":33}
        self.pub_dict = dict()
        self.pub_dict[7]   = {'nm':'exttemp'           , 'sc':10}
        self.pub_dict[8]   = {'nm':'sumwintemp'        , 'sc':10}
        self.pub_dict[9]   = {'nm':'antifreezeexttemp' , 'sc':10}
        #self.pub_dict[10]  = {'nm':'onoffstatepump'    , 'sc':1 }
        #self.pub_dict[11]  = {'nm':'speedpump'         , 'sc':1 }
        self.pub_dict[13]  = {'nm':'daysantifreeze'    , 'sc':1 }
        self.pub_dict[14]  = {'nm':'daytemp'           , 'sc':10}
        self.pub_dict[15]  = {'nm':'nighttemp'         , 'sc':10}
        self.pub_dict[16]  = {'nm':'antifreezetemp'    , 'sc':10}
        self.pub_dict[17]  = {'nm':'mode'              , 'sc':1 }
        #self.pub_dict[20]  = {'nm':'slope'             , 'sc':10}
        self.pub_dict[59] = {'nm':'watertempset' , 'sc':10}
        #self.pub_dict[89]  = {'nm':'baseecs'           , 'sc':1 }
        #self.pub_dict[94] = {'nm':'telecmd1'          , 'sc':1 }
        #self.pub_dict[95] = {'nm':'telecmd2'          , 'sc':1 }
        self.pub_dict[96] = {'nm':'watertempnight'    , 'sc':10 }
        #self.pub_dict[437] = {'nm':'onoffboilerflow'   , 'sc':1 }
        self.pub_dict[438] = {'nm':'largetemp'         , 'sc':10}
        self.pub_dict[451] = {'nm':'ionizationcurrent' , 'sc':10}
        self.pub_dict[452] = {'nm':'supplytemp'        , 'sc':10}
        self.pub_dict[453] = {'nm':'returntemp'        , 'sc':10}
        self.pub_dict[454] = {'nm':'exhausttemp'       , 'sc':10}
        self.pub_dict[455] = {'nm':'rpm'               , 'sc':1 }
        self.pub_dict[456] = {'nm':'pressure'          , 'sc':10}
        self.pub_dict[459] = {'nm':'watertemp'         , 'sc':10}
        self.mqtt = userdata
        self.reading = Thread(
            target=self.recv_send,
            args=(self.on_receive_callback, self.on_disconnected_callback)
        )
        #
        Thread.__init__(self)
        return


    # run by the Thread object
    def run(self):
        logger.info('Restart Modbus-Interface...')
        res = requests.post('http://192.168.178.7/reload_en.html',headers={'Authorization':'Basic XXXXXXXXX'},data="__SL_P_WRT=OK&__SL_P_WFT=NO&__SL_P_WUN.value=admin&__SL_P_WUP.value=admin")
        logger.info(res.text)
        logger.info('Wait 10s...')            
        time.sleep(10)
        logger.info('Modbus restarted')        
        #
        logger.info('Starting mydiematic...')
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(10)
        # self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # now connect
        try:
            logger.info('Connecting...')
            self.sock.connect((self.host, self.port))
            if self.on_connected_callback:
                self.on_connected_callback(self.host, self.port)
        except Exception as e:
            logger.info(f'Timeout during connection {e}')
            self.sock.close()
            return False
        # start a thread to read on socket
        self.reading.start()
        loop = True
        while loop:
            for i,_ in self.dictionary.items():
                self.read_request_queue.put_nowait(self.read_request(self.dictionary[i]['adr'],self.dictionary[i]['nb']))
                self.request = self.read_request_queue.get_nowait()
                read_request_task = Thread(target=self.read_request_queue.join())
                read_request_task.start()
                read_request_task.join(6) # blocks, timeout after 6s
            if not self.write_request_queue.empty():
                self.request = self.write_request_queue.get_nowait()
                logger.info(f'write request queued {self.request.hex()}')
                write_request_task = Thread(target=self.write_request_queue.join())
                write_request_task.start()
                write_request_task.join(6) # blocks, timeout after 6s
            if self.reading.is_alive():
                #logger.info(f'Health {self.read_request_queue.qsize()}{self.write_request_queue.qsize()}')
                # publish heartbeat as timestamp in unix timeformat for now
                self.mqtt.publish(self.mqtt_topic_root+"/heartbeat",int(time.time()), retain=True);
                continue
            else:
                logger.info('reading not alive')
                loop = False
                raise ValueError('reading not alive')
                break
        #logger.info('end of run')
        self.reading.join(0.1)
        self.sock.close()
        self.on_disconnected_callback()

    def read_request(self,regAddress,regNb):
       request=bytearray();
       request.append(self.ADDRESS);
       request.append(self.READ_ANALOG_HOLDING_REGISTERS);
       request.append((regAddress>>8)& 0xFF);
       request.append(regAddress & 0xFF);
       request.append((regNb>>8)& 0xFF);
       request.append(regNb & 0xFF);
       crc=calc_crc(request);
       request.append(crc & 0xFF);
       request.append((crc>>8)& 0xFF);
       request.append(0);
       logger.debug(f'Send read request: {request.hex()}');
       #self.sock.send(request);
       return request

    def write_request(self,regAddress,data=[0]):
        request=bytearray();
        request.append(self.ADDRESS)
        request.append(self.WRITE_MULTIPLE_REGISTERS);
        request.append((regAddress>>8)& 0xFF);
        request.append(regAddress & 0xFF);
        request.append(0x00);
        request.append(len(data)); # amount of registers to write
        request.append(2*len(data)); # number of bytes in
        for reg in data:
            request.append((reg>>8)& 0xFF);
            request.append(reg & 0xFF);
        crc=calc_crc(request);
        request.append(crc & 0xFF);
        request.append((crc>>8)& 0xFF);
        request.append(0x0);
        return request

    def recv_send(self, on_receive_callback, on_disconnected_callback):
        # set a buffer size ( could be 2048 or 4096 / power of 2 )
        size = 1024
        self.sock.settimeout(self.timeout)
        loop = True
        while loop:
            try:
                data = self.sock.recv(size)
                if data:
                    if on_receive_callback:
                        try:
                            on_receive_callback(self,data)
                        except Exception as e:
                            if self.debug:
                                logger.info(f'Receive Callback Failed {e}')
            except socket.timeout:
                try:
                    logger.debug(f'Send request {self.request.hex()}')
                    self.sock.send(self.request) #<---- From Queue
                except Exception as e:
                    logger.info('read/write request failed {e}')
                    raise ValueError('read/write request failed')
                    loop = False
                    break
            except Exception as e:
                logger.info(f'connection exception {e}')
                raise ValueError('connection exception')
                sys.exit()
                loop = False
                break
        return

def publish_callback(mydiematic,data):
    logger.debug(f'Received {data[2]}')
    mydiematic.mqtt.publish(mydiematic.mqtt_topic_root+"/estpvpower",estpower(datetime.datetime.now(pytz.timezone("Europe/Berlin"))));
    for _,item in mydiematic.dictionary.items():
        if data[2] == item.get('nb') * 2:
            adr_enum = 0
            poweron = False
            for i in range(0,data[2],2):
                logger.debug(f' {item.get("adr")+adr_enum} {0x100*data[3+i]+data[4+i]}')
                if (item.get('adr')+adr_enum) in mydiematic.pub_dict:
                    topic = mydiematic.mqtt_topic_root + '/' + mydiematic.pub_dict[item.get('adr')+adr_enum].get('nm')
                    if mydiematic.pub_dict[item.get('adr')+adr_enum].get('sc') == 1:
                        value = int(0x100*data[3+i]+data[4+i])
                        if value >= 0x8000:
                            value = -1.0*(value & 0x7fff)
                    else:
                        value = int(0x100*data[3+i]+data[4+i])
                        if value >= 0x8000:
                            value = -1.0*(value &  0x7fff)
                        value = value / mydiematic.pub_dict[item.get('adr')+adr_enum].get('sc')
                    logger.debug(f'{topic} {value}')
                    mydiematic.mqtt.publish(topic,round(value,2))
                if item.get('adr')+adr_enum == 451: # ionizaation current > 0 --> power on
                    if int(0x100*data[3+i]+data[4+i]) > 0:
                        poweron = True
                if item.get('adr')+adr_enum == 455:
                    value = 0.0
                    if poweron:
                        value = int(0x100*data[3+i]+data[4+i]) / 5900 * 24.5 * 0.61
                    topic = mydiematic.mqtt_topic_root + '/power'
                    mydiematic.mqtt.publish(topic,round(value,2))
                adr_enum = adr_enum + 1
    return

def receive_callback(mydiematic,data):
    logger.debug(f'Received {data.hex()}')
    #check  modBus address
    if (data[0] != mydiematic.ADDRESS):
        logger.debug('Answer modbus address Error');
        return;
    #check  modBus feature
    if (not ((data[1] == mydiematic.READ_ANALOG_HOLDING_REGISTERS) or (data[1] == mydiematic.WRITE_MULTIPLE_REGISTERS))):
        logger.debug('Answer modbus feature Error');
        return;
    # process ACK after writeing
    if data[1] == mydiematic.WRITE_MULTIPLE_REGISTERS:
        waited_ack=mydiematic.request[0:6];
        crc=calc_crc(waited_ack);
        waited_ack.append(crc & 0xFF);
        waited_ack.append((crc>>8)& 0xFF);
        if (waited_ack==data[0:8]):
            logger.info('Write Ack');
            mydiematic.write_request_queue.task_done()
        else:
            logger.info(f'Write NO Ack {data.hex()}')
        return
    # check for finishing a read request
    check = []
    for _,nb in mydiematic.dictionary.items():
        check.append(data[2] == nb.get('nb')*2)
    if any(check):
        try:
            mydiematic.read_request_queue.task_done()
        except:
            logger.info('received data but task empty')
    else:
        return
    # check lengt
    answerLength=5+data[2];
    if ((len(data) < answerLength )):
        logger.debug('Answer Length Error');
        return;
    # check CRC
    crc=calc_crc(data[0:answerLength-2]);
    if (crc!=0x100*data[answerLength-1]+data[answerLength-2]):
        logger.debug('Answer CRC error');
        return;
    publish_callback(mydiematic,data)
    return

def connected_callback(host,port):
    logger.info(f'Mydiematic connected: {host}:{port}')
    return

def disconnected_callback(msg=None):
    logger.info(f'Mydiematic stopped {msg}')
    return

def on_connect_mqtt(client, mydiematic, flags, rc):
    logger.info(f'Mqtt Client connected {rc}')
    client.subscribe(mydiematic.mqtt_topic_root+"/set/#")
    return

def on_disconnect_mqtt(client, userdata, rc):
    logger.info(f'Mqtt Client disconnected {rc}')
    return

def on_message_mqtt(client, mydiematic, msg):
    logger.info(f'{msg.topic} {msg.payload}')
    logger.debug(f'{msg.payload}')
    if '/'.join(msg.topic.split('/')[0:-1]) == mydiematic.mqtt_topic_root+'/set':
        write_request = None
        if msg.topic.split('/')[-1] == 'sumwin':
            write_request = mydiematic.write_request(8,[int(float(msg.payload)*10)])
        if msg.topic.split('/')[-1] == 'daytemp':
            write_request = mydiematic.write_request(14,[int(float(msg.payload)*10)])
        if msg.topic.split('/')[-1] == 'mode':
            write_request = mydiematic.write_request(17,[int(msg.payload)])
        if msg.topic.split('/')[-1] == 'telecmd1':
            write_request = mydiematic.write_request(94,[int(msg.payload)])
        if msg.topic.split('/')[-1] == 'watertempnight':
            write_request = mydiematic.write_request(96,[int(float(msg.payload)*10)])
        if msg.topic.split('/')[-1] == 'watertempset':
            write_request = mydiematic.write_request(59,[int(float(msg.payload)*10)])
        if msg.topic.split('/')[-1] == 'nighttemp':
            write_request = mydiematic.write_request(15,[int(float(msg.payload)*10)])
        if write_request:
            try:
                mydiematic.write_request_queue.put_nowait(write_request)
            except:
                logger.info('Write queue is full, wait and send again')
    return


def estpower(date):
    latitude = 0.0
    longitude = 0.0
    beta_pv = 70/180*math.pi
    alpha_pv = 200/180*math.pi
    area = 8.0 * 0.95

    #date = datetime.datetime.now(pytz.timezone("Europe/Berlin"))
    beta_sun = get_altitude(latitude, longitude, date)/180*math.pi
    alpha_sun = get_azimuth(latitude, longitude, date)/180*math.pi

    pv = np.array([math.sin(alpha_pv),-math.cos(alpha_pv),math.sin(beta_pv)])
    sun = np.array([math.sin(alpha_sun),-math.cos(alpha_sun),math.sin(beta_sun)])

    incident_angle = math.acos(np.dot(pv,sun)/np.linalg.norm(pv)/np.linalg.norm(sun))*180/math.pi;
    # model
    if incident_angle >=0 and incident_angle <= 80:
        eta = 22 - (1/(math.exp(0.0015*(95-incident_angle)**1.3)-1))
    else:
        eta = 1
    theoretic_power = radiation.get_radiation_direct(date, beta_sun*180/math.pi) * area * eta/100;

    return theoretic_power



def main():
    logger.info('init')
    mqtt = paho.Client()
    mqtt.on_connect = on_connect_mqtt
    mqtt.on_message = on_message_mqtt
    mqtt.on_disconnect = on_disconnect_mqtt
    mqtt.connect("127.0.0.1", 1883, 60)
    mqtt.loop_start()
    logger.info('Mqtt Client started')
    stop_signal = Event()
    mydiematic = DiematicConnect(
        stop_signal,
        '192.168.178.7',
        88,
        mqtt_topic_root='home/heating',
        timeout=0.7,
        on_connected_callback=connected_callback,
        on_receive_callback=receive_callback,
        on_disconnected_callback=disconnected_callback,
        userdata=mqtt,
    )
    mqtt.user_data_set(mydiematic)
    killer = GracefulKiller()
    mydiematic.start()
    #mydiematic.join(1)
    while not killer.kill_now:
        if not mydiematic.reading.is_alive():
            logger.info('Try to restart...')
            raise ValueError('Restart')
        time.sleep(2)
    stop_signal.set()
    mqtt.loop_stop()
    mqtt.disconnect()
    #mydiematic.join(10)
    logger.info("End of mydiematic.")
    return

if __name__ == "__main__":
    main()
