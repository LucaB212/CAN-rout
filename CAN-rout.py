import can
import struct

from can.interfaces import socketcan
import json
import time
import logging
from logging.handlers import RotatingFileHandler 
import threading
from queue import Queue,PriorityQueue
import os

import sys

global lonlat
global timestamp

global infoMessage
global dataLogger

if sys.argv[1]:
    db_level = sys.argv[1]
else:
    db_level = 'ERROR'


#### INITIALIZATION  ####
#data_list = []

handler = RotatingFileHandler("canrout.log", mode='a', maxBytes=100000, backupCount=1)
logging.basicConfig(format='%(asctime)s - %(funcName)s - %(message)s', level=db_level, handlers=[handler])
telem_buff = Queue()        # used for event with satus code E003
state_buff = Queue()        # used for event with status code E000
canSend_ordered = PriorityQueue()

confFilename = "CAN-rout-conf.json"
none = 0
statistics = 4
count = 8
variations = 32
activations = 64



lastid = -1
fifo_path = "/temp/sybok/events"
fifoDL_path = "/temp/sybok/datalog"



min_value = []
max_value = []
sum_value = []
sum2_value = []
last_value = []
sample_list = []
vars_value = []
actv_value = []

canId_CMTP = 0x18EC00FE
canId_DTTP = 0x18EB00FE
CMTP_header = [0xA5, 0x5A]

infoMessage_name = ["INFO1","INFO2","INFO3"]    #message name to be considered as Info. Add here new names. This structure follow the "INFO"
dataLogger_name = ["STAB1","STAB2"]                  #message name to be considered as DataLogger.

lonlat = "0000000000000000"
timestamp = "00000000"

########################

def conf_extract():
    #set_filter_list = []
    data_router_dict = {}
    """Open and extract all the CAN filters"""
    try:
        with open(confFilename, 'r') as f:
            data_router_dict = json.load(f)
            logging.info("Configuration file extracted succesfully")
    except FileNotFoundError as e:
        logging.error("Configuration file not found with name:{}".format(confFilename))
        raise(e)

    return data_router_dict



def eventManager(buff_3, buff_0):       ### added event for start the timer

    ####    Thread function   #####
    '''This function takes the event from the fifo
    and it parses only the one with accepted Status code. Then it 
    put the data inside queue buffer depending on the status code.

    WARNING: the data pushed in the queue has to be grouped inside each
    packet looking at the first byte header: 4 bit representing the num of
    consecutive byte, 4 bit representing the number of the packet (from 0)

     0:1 | header
     1:2 | NOT USED
     3:2 | NOT USED
     5:2 | NOT USED

    
    The HEADER has to be used to group togheter multiple packets in one unique structure.
    Then the next 6 bytes must be discarded.
    From byte 7 (from 0) starts the CAN id
    '''
    logging.info("Thread start")
    #print("Thread 1 start")
    ### Initialization  ###
    event_buff = ""
    lastid = -1
    status_codes = ["E003", "E000"]
    global timestamp
    global lonlat
    

    while True:

        with open(fifo_path) as pipein:

            for event in pipein:
                logging.debug("Event :  {}".format(event))
                #print("Event : {}".format(event))
                event_split = event.split(',')

                
                if event_split[3] in status_codes:

                    ### TRY EVENT ###
                    #logging.info("Event received with <<Status code  {}>>".format(event_split[3]))

                   
                    #################
                    lonlat = event_split[2] + event_split[1]    ###
                    timestamp = event_split[0]                  ###
                    
                    numOfPacket = int(event_split[4][0],base=16)
                    idOfPacket = int(event_split[4][1],base=16)
                    logging.debug("Num of packet: {}".format(numOfPacket))
                    logging.debug("ID of packet: {}".format(idOfPacket))
                    #print("Num of packet: {}".format(numOfPacket))
                    #print("ID of packet: {}".format(idOfPacket))

                    if idOfPacket == 0:
                        event_buff += event_split[4][14:-1]
                    else:
                        event_buff += event_split[4][2:]

                    if (numOfPacket == (idOfPacket + 1)) and (lastid == idOfPacket - 1):
                        #telem_event = []
                        #telem_event.append(event_split[4][14:22])
                        #telem_event.append(event_split[4][22:])
                        if event_split[3] == "E003":
                            buff_3.put(event_buff)
                            logging.debug("event added to buff_3: {}".format(event_buff))
                            #print("event buff_3: {}".format(event_buff))
                        elif event_split[3] == "E000":
                            buff_0.put(event_buff)
                            logging.debug("event added to buff_0: {}".format(event_buff))
                            #print("event buff_0: {}".format(event_buff))

                        event_buff = ""
                        lastid = -1

                    elif idOfPacket != lastid +1 :
                        event_buff = ""
                        lastid = -1

                    else :   
                        lastid = idOfPacket

def dataLoggerManager(data_router_dict):
    #### THREAD FUNCTION ###
    '''This function takes the packet from the datalogger and produces the stats'''
    logging.info("Thread start")
    #print("Thread 7 start")

    global dataLogger

    with open(fifoDL_path) as pipein:
        logline = ""
        
        while len(logline) != 140:
            logline = pipein.readline()[:-1]

        logging.debug("Buffer: {}".format(logline))

        logline_split = logline.split(',')

        for packet in data_router_dict["DTLOG"]:
                                 
            buff = ""
            for byte in logline_split[packet["byte"][0]:packet["byte"][1]]:
                buff += byte
            size = len(buff) << 2

            buff = format(int(buff, base=16), '0>{}b'.format(size))
            data = buff[packet["fromBit"]:packet["toBit"]]

            logging.debug("Data extracted: {} from {} with size: {}".format(data,packet["byte"], size))

            try:
                dataLogger[data_router_dict[packet["name"]]["dataLogger"]][packet["start"]] = data
            except IndexError:
                dataLogger[data_router_dict[packet["name"]]["dataLogger"]] = data_router_dict[packet["name"]]["data_list"]
                dataLogger[data_router_dict[packet["name"]]["dataLogger"]][packet["start"]] = data

        for i in range(len(dataLogger)):

            buff = ""
        
            for elem in dataLogger[i]:
                buff += elem
            
            
            if len(buff):
                logging.debug("<<{} buffer  {}>>".format(i,buff))
                #print("specialMessageSender -> buff: {}".format(buff))  
                canSend_ordered.put((data_router_dict[dataLogger_name[i]]["priority"],[(int(data_router_dict[dataLogger_name[i]]["can_id"],base=16),byteArrayProducer(format(int(buff,base=2),'0>16x')))]))
                dataLogger[i] = data_router_dict[dataLogger_name[i]]["data_list"]



def canSendManager(buff):

    ###  THREAD FUNCTION  ###
    '''This function get the can frame to be sent over the can interface 2.
    The buff is a priority queue and the data are packed like this
    [(can_id,data_frame#1),
    (can_id,data_frame#2),
    (can_id,data_frame#3)]
    '''
    logging.info("Thread start")
    #print("Thread 2 start")
    time_send = 0

    while True:
        msg_to_send = buff.get()      #it blocks undefinetely untill a new can message is ready to be sent
        logging.debug("Message received: {}".format(msg_to_send))
        #print("Msg to sd: {}".format(msg_to_send))
        
        try:
            time.sleep(0.1 - (time.time() - time_send))
        except ValueError:
            pass

        for payload in msg_to_send[1]:
            #time.sleep(0.05)
            #timestamp = time.time()
            msg = can.Message(arbitration_id=payload[0],
                            data=payload[1],
                            is_extended_id=True,
                            dlc=8)
            try:
                time.sleep(0.05 - (time.time() - time_send))
            except ValueError:
                pass
                
            try:
                bus.send(msg)
                time_send = time.time() 
                #logging.info("TIME PASSED >> {}".format(time))
                logging.info("Message sent on {} with:\n\t\t<<CAN-ID  {}>>\n\t\t<<DATA  {}>>".format(bus.channel_info,hex(payload[0]),payload[1]))
                #print("Message sent on {} with payload {}".format(bus.channel_info, payload))
            except can.CanError:
                logging.error("BUSHEAVY >> message not sent on {}".format(bus.channel_info))
                #print("Message NOT sent")

        
def telemVehicle(telem_buff, data_router_dict, canSend_ordered, lock, infoFirstEvent):
    #### THREAD FUNCTION ####
    ''' This function get the telem event with status code E003 and put the message to send on the can'''

    logging.info("Thread start")
    #print("Thread 5 start")
    global infoMessage

    while True:

        msg = telem_buff.get()  ###it bloks indefinitely untill a new data is available 
        jump = False        ##jump flag: if set disard the current msg 

        '''
         0:4 | CAN ID
         4:1 | Num of variables in the packet

        '''

        can_id = msg[0:8]
        logging.debug("<<CAN-ID  {}>>".format(can_id))
        #print("Can ID: {}".format(can_id))

        if can_id in data_router_dict.keys():       
        ###Check if the can id is present in the configuration file

            num_vars = int(msg[8:10], base=16)   #num of variables in the telemetry event
            logging.debug("<<Num of variables  {}>>".format(num_vars))
            #print("Num vars: {}".format(num_vars))

            msg = msg[10:]  #cut the msg

            
            for var in range(num_vars):

                '''
                byte| function
                0:1 | FLAGS (0x00, 0x04, 0x08, 0x20, 0x40)
                1:3 | Last valid value rec
                3:5 | Num of valid values
                5:7 | Num of out of range values
                
                '''
                logging.debug("<<Variable num  {}>>".format(var))
                logging.debug("<<Msg  {}>>".format(msg))
                #print("Num of variable: {}".format(var))
                #print("Msg: {}".format(msg))

                flags = int(msg[0:2],base=16)
                
                last_value, num_of_valid, num_out_of_range = eventFactNone(msg[2:14])

                logging.debug("<<Flags  {}>>".format(flags))
                logging.debug("<<Last value  {}>>".format(last_value))
                logging.debug("<<Num of valid  {}>>".format(num_of_valid))
                #print("flags: {}".format(flags))
                #print("Last value: {}".format(last_value))
                #print("Num of valid: {}".format(num_of_valid))

                if (flags == none):
                    
                    msg = msg[14:]

                elif (flags & statistics) == statistics:
                    sum_value, sum2_value, min_value, max_value = eventFactStat(msg[14:38])

                    if (flags & variations) == variations:
                        vars_value = eventFactVar(msg[38:42])
                        #if i != num_vars - 1:
                        msg = msg[42:]

                    elif (flags & activations) == activations:
                        actv_value = eventFactAct(msg[38:42])

                        #if i != num_vars - 1:
                        msg = msg[42:]

                    else:
                        msg = msg[38:]

                elif (flags & count) == count:
                    sample_list, end_val = eventFactCount(msg[14:])

                    if (flags & variations) == variations:
                        vars_value = eventFactVar(msg[end_val:(end_val+4)])

                        msg = msg[(end_val+4):]
                        
                    elif (flags & activations) == activations:
                        actv_value = eventFactAct(msg[end_val:(end_val+4)])

                        msg = msg[end_val+4:]
                    else:
                        msg = msg[end_val:]

                elif (flags & variations) == variations:

                    vars_value = eventFactVar(ms[14:18])    

                elif (flags & activations) == activations:

                    actv_value = eventFactAct(msg[14:18])  


                stats = data_router_dict[can_id][var]["type"]

                if stats:
                    canSendBuff = []

                    if "val_imm" in stats:  
                        ###specific can id protocol
                        #canSendBuff.append()
                        #logging.info("New infomessage added")
                        #print("val_imm condition passed")

                        last_value_b = format((last_value[0] * 256 + last_value[1]), '0>{}b'.format(data_router_dict[can_id][var]["len"]))

                        with lock:
                            logging.info("Lock acquire for\t<<INFOMESSAGE  {}>>\t<<POSITION  {}>>".format(data_router_dict[can_id][var]["name"], data_router_dict[can_id][var]["start"]))
                            try:
                                infoMessage[data_router_dict[data_router_dict[can_id][var]["name"]]["infoMessage"]][data_router_dict[can_id][var]["start"]] = last_value_b[-data_router_dict[can_id][var]["len"]:]
                            except IndexError:
                                infoMessage[data_router_dict[data_router_dict[can_id][var]["name"]]["infoMessage"]] = data_router_dict[data_router_dict[can_id][var]["name"]]["data_list"]
                                infoMessage[data_router_dict[data_router_dict[can_id][var]["name"]]["infoMessage"]][data_router_dict[can_id][var]["start"]] = last_value_b[-data_router_dict[can_id][var]["len"]:]

                            if not infoFirstEvent.is_set():
                                infoFirstEvent.set()    #notify the specialMessageSender that one field in the infoMessage struct has been filled
                                logging.info("<<infoFirstEvent  SET>>")
                    else:

                        canSendBuff.append((canId_CMTP,(CMTP_header + 
                                                        swap_byte_order(format(data_router_dict[can_id][var]["dlc"], '0>4X')) + 
                                                        swap_byte_order(format(data_router_dict[can_id][var]["packs"], '0>2X')) + 
                                                        data_router_dict[can_id][var]["id"])))

                        logging.debug("<<canSendBuff inserted  {}>>".format(canSendBuff))
                        #print("canSendBuff: {}".format(canSendBuff))

                        buff = []
                        for stat in stats:
                            if stat == "cnt":
                                
                                for i in range(data_router_dict[can_id][var]["cnt"]):
                                    buff += sample_list[i]
                                buff += num_out_of_range

                            elif stat == "attv":
                                buff += actv_value

                            elif stat == "varz":
                                buff += vars_value

                            elif stat == "val_8":
                                buff += [last_value[0]]

                            elif stat == "val_16":
                                buff += last_value

                            elif stat == "min":
                                buff += min_value

                            elif stat == "max":
                                buff += max_value

                            elif stat == "med":
                                buff += sum_value

                            elif stat == "rms":
                                buff += sum2_value
                                buff += num_of_valid

                            else:
                                logging.info("Invalid statistics\n\t\t<<CAN-ID  {}>>\t<<VAR  {}>>\n\t\t!!EXITING FROM THE PARSING!!".format(can_id,var))
                                jump = True
                                break
                                          
                            logging.debug("<<Intermediate buff  {}>>".format(buff))
                            #print("Intermediate Buff: {}".format(buff))

                        if not jump:
                            num_of_packet = data_router_dict[can_id][var]["packs"]
                            res_mask_B = format(data_router_dict[can_id][var]["res_mask_B"], '0>{}b'.format(num_of_packet * 8))
                            logging.debug("<<Reserved mask  {}>>".format(res_mask_B))
                            #print("Reserved mask: {}".format(res_mask_B))
                            #print("#### res_mask_B: {}".format(data_router_dict[can_id][var]["res_mask_B"]))

                            for i in range(num_of_packet * 8):
                                if res_mask_B[i] == '1':
                                    buff.insert(i,0xFF) #reserved field insertion
                                
                                if not(i % 8):
                                    buff.insert(i,int(i/8)+1)  #header insertion
                            
                            logging.debug("<<Final Buff  {}".format(buff))
                            #print("Final Buff: {}".format(buff))



                            for i in range(num_of_packet):
                                canSendBuff.append((canId_DTTP,buff[i*8:(i+1)*8]))


                            logging.debug("<<CanSendBuff inserted  {}".format(canSendBuff))
                            #print("CanSendBuff: {}".format(canSendBuff))
                            canSend_ordered.put((data_router_dict[can_id][-1],canSendBuff))
                        else:
                            break       ##jump to the msg.get() wait
                
def stateVehicle(state_buff, data_router_dict, lock, infoFirstEvent):
     #### THREAD FUNCTION ####
    ''' This function get the telem event with status code E000. This events have only "val_imm" statistics.'''

    logging.info("Thread start")
    #print("Thread 6 start")
    global infoMessage

    while True:
        msg = state_buff.get()      # blocks untill a new E000 event arrive

        can_id = msg[0:8]
        logging.debug("<<CAN-ID  {}>>".format(can_id))
        #print("Can ID: {}".format(can_id))

        if can_id in data_router_dict.keys():       
            ###Check if the can id is present in the configuration file

            last_value_b = format(int(msg[8:], base=16), '0>64b')
            last_value_b = last_value_b[data_router_dict[can_id][0]["position"][0]:data_router_dict[can_id][0]["position"][1]]
            #logging.info("New infomessage added")

            logging.debug("<<Last value  {}".format(last_value_b))
            #print("stateVehicle -> last_value_b: {}".format(last_value_b))
            with lock:
                logging.info("Lock acquire for\t<<INFOMESSAGE  {}>>\t<<POSITION  {}>>".format(data_router_dict[can_id][0]["name"], data_router_dict[can_id][0]["start"]))
                try:
                    infoMessage[data_router_dict[data_router_dict[can_id][0]["name"]]["infoMessage"]][data_router_dict[can_id][0]["start"]] = last_value_b
                except IndexError:
                    infoMessage[data_router_dict[data_router_dict[can_id][0]["name"]]["infoMessage"]] = data_router_dict[data_router_dict[can_id][0]["name"]]["data_list"]
                    infoMessage[data_router_dict[data_router_dict[can_id][0]["name"]]["infoMessage"]][data_router_dict[can_id][0]["start"]] = last_value_b

                if not infoFirstEvent.is_set():
                    infoFirstEvent.set()        #notify the specialMessageSender that one field in the infoMessage struct has been filled
                    logging.info("<<infoFirstEvent  SET>>")

def latlontimeSender(data_router_dict, buff):
    ### THREAD FUNCTION ###
    ''' This function is a timer that every 60'' takes the latlon value and the date and creates the can messages'''
    logging.info("Thread start")
    #print("Thread 3 start")

    global timestamp
    global lonlat
    
    data_date = "FFFFFFFF" + timestamp 

    logging.debug("Prepared message\n\t\t<<LONLAT  {}>>\t<<DATE&TIME  {}>>".format(lonlat, data_date))
    #print("lonlat: {}".format(lonlat))
    #print("date and time: {}".format(data_date))
    buff.put((data_router_dict["DT"]["priority"],[(int(data_router_dict["DT"]["can_id"],base=16), swap_byte_order(data_date))]))          
    buff.put((data_router_dict["LATLON"]["priority"],[(int(data_router_dict["LATLON"]["can_id"],base=16), swap_byte_order(lonlat))]))        

def specialMessageSender(data_router_dict, canSend_ordered, firstevent, lock):
    ### THREAD FUNCTION ###
    ''' This function uses the thread event to be notified when is time to send the special messages'''

    logging.info("Thread start")
    #print("Thread 4 start")
    global infoMessage

    firstevent.wait()    

    time.sleep(10)      #time to wait before we receive the first field
    firstevent.clear()
    logging.info("<<infoFirstEvent  UNSET>>")

    with lock:
        logging.info("Lock acquire >> prepare for sending InfoMessage")
        for i in range(len(infoMessage)):

            buff = ""
            
            for elem in infoMessage[i]:
                buff += elem
            
            
            if len(buff):
                logging.debug("<<{} buffer  {}>>".format(i,buff))
                #print("specialMessageSender -> buff: {}".format(buff))  
                canSend_ordered.put((data_router_dict[infoMessage_name[i]]["priority"],[(int(data_router_dict[infoMessage_name[i]]["can_id"],base=16),byteArrayProducer(format(int(buff,base=2),'0>16x')))])) ### infox are the buff for the 3 messages to be completed in "val_imm" checkswap_byte_order(int(buff, base=16))
                infoMessage[i] = data_router_dict[infoMessage_name[i]]["data_list"]
            

def eventFactAct(msg):
    '''This function extracts the data from the buffer and produces the Activations stats'''
    
    return swap_byte_order(msg)


def eventFactVar(msg):
    '''This function extracts the data from the buffer and produces the Variations stats'''
    return swap_byte_order(msg)


def eventFactNone(msg):
    '''This function extracts the data from the buffer and produces the None stats'''

    return swap_byte_order(msg[0:4]),swap_byte_order(msg[4:8]),swap_byte_order(msg[8:])


def eventFactStat(msg):
    '''This function extracts the data from the buffer and produces the Statistics stats'''

    return swap_byte_order(msg[0:8]), swap_byte_order(msg[8:16]), swap_byte_order(msg[16:20]), swap_byte_order(msg[20:24]) #sum_value, sum2_value, min_value, max_value

def eventFactCount(msg):
    '''This function extracts the data from the buffer and produces the Counts stats'''
    mask_val = int(msg[0:4],base=16)
    mask_bin = format(mask_val, '0>16b')
    num_val = bin(mask_val).count("1")
    end_val = num_val*4+18
    buff = []
    msg = msg[4:]
    
    #print("EventFactCount -> mask_val: {}".format(mask_val))
    #print("EventFactCount -> num_val: {}".format(num_val))
    #print("EventFactCount -> end_val: {}".format(end_val))

    for i in range(15,-1,-1):
        if mask_bin[i] == '1':
            buff.append(swap_byte_order(msg[:4]))
            msg = msg[4:]
        else:
            buff.append([0,0])
        

    return buff, end_val


def swap_byte_order(msg):
    ''' this for cycle extracts the bytes from the data field and it appends them in little-endian order inside the list for the can send'''
    buff= []
    for i in range(len(msg)-1,0,-2):                   
        buff.append(int(msg[i-1:i+1],base=16))

    return buff

def byteArrayProducer(msg):
    ''' this for cycle takes a string in hex format and retrieves a bytearray'''

    buff = []

    for i in range(0,len(msg),2):
        buff.append(int(msg[i:i+2],base=16))

    return buff





if __name__ == "__main__":

    data_router_dict = conf_extract()
    #print("data router dict: {}".format(data_router_dict))

    infoMessage = data_router_dict["INFO"]          ## this is the structure used with an empty list of list
    dataLogger = data_router_dict["LOG"]


    with can.interfaces.socketcan.SocketcanBus(channel="can1", receive_own_messages=False, fd=False) as bus:
        logging.info("SocketcanBus open on channel <<can1>>")
        
        
        infoFirstEvent = threading.Event()
        infoMessageLock = threading.Lock()

        

        thread1 = threading.Thread(target=eventManager, args=(telem_buff, state_buff,), daemon=True)
        thread1.start()
        thread2 = threading.Thread(target=canSendManager, args=(canSend_ordered,), daemon=True)
        thread2.start()
        thread3 = threading.Timer(function=latlontimeSender, args=(data_router_dict, canSend_ordered,), interval=60.0)
        thread3.start()
        thread4 = threading.Thread(target=specialMessageSender, args=(data_router_dict, canSend_ordered, infoFirstEvent, infoMessageLock,), daemon=True)
        thread4.start()
        thread5 = threading.Thread(target=telemVehicle, args=(telem_buff, data_router_dict, canSend_ordered, infoMessageLock, infoFirstEvent,), daemon=True)
        thread5.start()
        thread6 = threading.Thread(target=stateVehicle, args=(state_buff, data_router_dict, infoMessageLock, infoFirstEvent,), daemon=True)
        thread6.start()
        thread7 = threading.Timer(function=dataLoggerManager, args=(data_router_dict,), interval=2.5)
        thread7.start()

        

        
        while True:
            
            if not thread1.is_alive():
                thread1 = threading.Thread(target=eventManager, args=(telem_buff,state_buff,), daemon=True)
                thread1.start()

            if not thread2.is_alive():
                thread2 = threading.Thread(target=canSendManager, args=(canSend_ordered,), daemon=True)
                thread2.start()

            if not thread3.is_alive():
                #rearm the timer
                thread3 = threading.Timer(function=latlontimeSender, args=(data_router_dict, canSend_ordered,), interval=60.0)
                thread3.start()

            if  not thread4.is_alive():
                thread4 = threading.Thread(target=specialMessageSender, args=(data_router_dict, canSend_ordered, infoFirstEvent, infoMessageLock), daemon=True)
                thread4.start()

            if not thread5.is_alive():
                thread5 = threading.Thread(target=telemVehicle, args=(telem_buff, data_router_dict, canSend_ordered, infoMessageLock, infoFirstEvent,), daemon=True)
                thread5.start()

            if not thread6.is_alive():
                thread6 = threading.Thread(target=stateVehicle, args=(state_buff, data_router_dict, infoMessageLock, infoFirstEvent,), daemon=True)
                thread6.start()

            if not thread7.is_alive(): 
                thread7 = threading.Timer(function=dataLoggerManager, args=(data_router_dict,), interval=2.5)
                thread7.start()

            time.sleep(1)

