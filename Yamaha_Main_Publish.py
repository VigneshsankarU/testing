from pymodbus.client.sync import ModbusTcpClient
import datetime
import time
import paho.mqtt.client as mqtt
import logging
import logging.handlers as handlers
import threading
import json

global A, B, C


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.connection_flag = True
        print("connected OK Returned code=", rc)
    else:
        client.bad_connection_flag = True
        print("Bad connection Returned code=", rc)


def on_message(client, userdata, message):
    print("Message Published to...", str(message.payload.decode("utf-8")))
    pass


def on_publish(client, userdata, result):
    print("data published \n")
    pass


def Camera_Inspection_Thread(Connection):
    A1 = 1
    A2 = 1
    print("inside Camera_Inspection function.... ")
    Camera_Inspection_Status = Connection.connect()
    time.sleep(0.5)
    while True:
        try:
            if Camera_Inspection_Status is True:
                Camera_Inspection = Connection.read_holding_registers(600, 2, unit=1).registers
                time.sleep(0.2)
                IDLE = Connection.read_holding_registers(603, 3, unit=1).registers
                time.sleep(0.2)

                if Camera_Inspection[0] == 1 and A1 == 1:
                    Data_Msg = {"Equipment_Name": "Camera_Inspection",
                                "Tag_Name": "START",
                                "Value": Camera_Inspection[0],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/1", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                    A1 = 0
                if Camera_Inspection[1] == 1 and A1 == 0:
                    Data_Msg = {"Equipment_Name": "Camera_Inspection",
                                "Tag_Name": "STOP",
                                "Value": Camera_Inspection[1],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/1", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                    A1 = 1
                if IDLE[0] == 1 and A2 == 1:
                    Data_Msg = {"Equipment_Name": "Camera_Inspection",
                                "Tag_Name": "IDLE_ON",
                                "Value": IDLE[0],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/1", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                    A
                if IDLE[2] == 1:
                    Data_Msg = {"Equipment_Name": "Camera_Inspection",
                                "Tag_Name": "IDLE_OFF",
                                "Value": IDLE[2],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/1", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish

        except Exception as ex:
            logger.error("exception in device connection : " + str(ex))
            print(ex)
            if Camera_Inspection_Status is False:
                Camera_Inspection_Status =Connection.connect()
                print("Camera_Inspection Connection Status is :" + str(Camera_Inspection_Status))
                time.sleep(1)
            else:
                pass


def Converter_1_Thread(Connection):
    print("inside Converter_1 function.... ")
    Converter_1_Status = Connection.connect()
    time.sleep(0.2)
    while True:
        try:
            if Converter_1_Status is True:
                Magnetizing_MC = Connection.read_holding_registers(555, 4, unit=2).registers
                time.sleep(0.2)
                Balancing = Connection.read_holding_registers(4096, 4, unit=8).registers
                time.sleep(0.2)
                IG_Marking = Connection.read_holding_registers(555, 4, unit=4).registers
                time.sleep(0.2)
                Pierce = Connection.read_holding_registers(4100, 4, unit=8).registers
                time.sleep(0.2)
                Riveting_MC = Connection.read_holding_registers(555, 4, unit=3).registers
                time.sleep(0.2)
                Boss_Pressing = Connection.read_holding_registers(555, 4, unit=5).registers
                time.sleep(0.2)

                if Magnetizing_MC[0] == 1:
                    Data_Msg = {"Equipment_Name": "Magnetizing_MC",
                                "Tag_Name": "START",
                                "Value": Magnetizing_MC[0],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Magnetizing_MC[1] == 1:
                    Data_Msg = {"Equipment_Name": "Magnetizing_MC",
                                "Tag_Name": "STOP",
                                "Value": Magnetizing_MC[1],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Magnetizing_MC[2] == 1:
                    Data_Msg = {"Equipment_Name": "Magnetizing_MC",
                                "Tag_Name": "IDLE_ON",
                                "Value": Magnetizing_MC[2],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Magnetizing_MC[3] == 1:
                    Data_Msg = {"Equipment_Name": "Magnetizing_MC",
                                "Tag_Name": "IDLE_OFF",
                                "Value": Magnetizing_MC[3],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish

                if Balancing[0] == 1:
                    Data_Msg = {"Equipment_Name": "Pierce&Balancing",
                                "Tag_Name": "START_M2",
                                "Value": Balancing[0],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Balancing[1] == 1:
                    Data_Msg = {"Equipment_Name": "Pierce&Balancing",
                                "Tag_Name": "STOP_M2",
                                "Value": Balancing[1],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Balancing[2] == 1:
                    Data_Msg = {"Equipment_Name": "Pierce&Balancing",
                                "Tag_Name": "IDLE_ON_M2",
                                "Value": Balancing[2],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Balancing[3] == 1:
                    Data_Msg = {"Equipment_Name": "Pierce&Balancing",
                                "Tag_Name": "IDLE_OFF_M2",
                                "Value": Balancing[3],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish

                if IG_Marking[0] == 1:
                    Data_Msg = {"Equipment_Name": "IG_Marking",
                                "Tag_Name": "START",
                                "Value": IG_Marking[0],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if IG_Marking[1] == 1:
                    Data_Msg = {"Equipment_Name": "IG_Marking",
                                "Tag_Name": "STOP",
                                "Value": IG_Marking[1],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if IG_Marking[2] == 1:
                    Data_Msg = {"Equipment_Name": "IG_Marking",
                                "Tag_Name": "IDLE_ON",
                                "Value": IG_Marking[2],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if IG_Marking[3] == 1:
                    Data_Msg = {"Equipment_Name": "IG_Marking",
                                "Tag_Name": "IDLE_OFF",
                                "Value": IG_Marking[3],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish

                if Pierce[0] == 1:
                    Data_Msg = {"Equipment_Name": "Pierce&Balancing",
                                "Tag_Name": "START_M1",
                                "Value": Pierce[0],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Pierce[1] == 1:
                    Data_Msg = {"Equipment_Name": "Pierce&Balancing",
                                "Tag_Name": "STOP_M1",
                                "Value": Pierce[1],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Pierce[2] == 1:
                    Data_Msg = {"Equipment_Name": "Pierce&Balancing",
                                "Tag_Name": "IDLE_ON_M1",
                                "Value": Pierce[2],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Pierce[3] == 1:
                    Data_Msg = {"Equipment_Name": "Pierce&Balancing",
                                "Tag_Name": "IDLE_OFF_M1",
                                "Value": Pierce[3],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish

                if Riveting_MC[0] == 1:
                    Data_Msg = {"Equipment_Name": "Riveting_MC",
                                "Tag_Name": "START",
                                "Value": Riveting_MC[0],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Riveting_MC[1] == 1:
                    Data_Msg = {"Equipment_Name": "Riveting_MC",
                                "Tag_Name": "STOP",
                                "Value": Riveting_MC[1],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Riveting_MC[2] == 1:
                    Data_Msg = {"Equipment_Name": "Riveting_MC",
                                "Tag_Name": "IDLE_ON",
                                "Value": Riveting_MC[2],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Riveting_MC[3] == 1:
                    Data_Msg = {"Equipment_Name": "Riveting_MC",
                                "Tag_Name": "IDLE_OFF",
                                "Value": Riveting_MC[3],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish

                if Boss_Pressing[0] == 1:
                    Data_Msg = {"Equipment_Name": "Boss_Pressing",
                                "Tag_Name": "START",
                                "Value": Boss_Pressing[0],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Boss_Pressing[1] == 1:
                    Data_Msg = {"Equipment_Name": "Boss_Pressing",
                                "Tag_Name": "STOP",
                                "Value": Boss_Pressing[1],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Boss_Pressing[2] == 1:
                    Data_Msg = {"Equipment_Name": "Boss_Pressing",
                                "Tag_Name": "IDLE_ON",
                                "Value": Boss_Pressing[2],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/2", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Boss_Pressing[3] == 1:
                    Data_Msg = {"Equipment_Name": "Boss_Pressing",
                                "Tag_Name": "IDLE_OFF",
                                "Value": Boss_Pressing[3],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/3", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                        
        except Exception as ex:
            logger.error("exception in device connection : " + str(ex))
            print(ex)
            if Converter_1_Status is False:
                Converter_1_Status = Connection.connect()
                print("Converter_1 Connection Status is :" + str(Converter_1_Status))
                time.sleep(1)
            else:
                pass


def Converter_2_Thread(Connection):
    print("inside Converter_2 function.... ")
    Converter_2_Status = Connection.connect()
    time.sleep(0.2)
    while True:
        try:
            if Converter_2_Status is True:
                Adhesive_1 = Connection.read_holding_registers(555, 4, unit=1).registers
                time.sleep(0.2)
                Adhesive_2 = Connection.read_holding_registers(555, 4, unit=6).registers
                time.sleep(0.2)
                if Adhesive_1[0] == 1:
                    Data_Msg = {"Equipment_Name": "Adhesive_1",
                                "Tag_Name": "START",
                                "Value": Adhesive_1[0],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/3", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Adhesive_1[1] == 1:
                    Data_Msg = {"Equipment_Name": "Adhesive_1",
                                "Tag_Name": "STOP",
                                "Value": Adhesive_1[1],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/3", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Adhesive_1[2] == 1:
                    Data_Msg = {"Equipment_Name": "Adhesive_1",
                                "Tag_Name": "IDLE_ON",
                                "Value": Adhesive_1[2],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/3", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Adhesive_1[3] == 1:
                    Data_Msg = {"Equipment_Name": "Adhesive_1",
                                "Tag_Name": "IDLE_OFF",
                                "Value": Adhesive_1[3],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/3", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish

                if Adhesive_2[0] == 1:
                    Data_Msg = {"Equipment_Name": "Adhesive_2",
                                "Tag_Name": "START",
                                "Value": Adhesive_2[0],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/3", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Adhesive_2[1] == 1:
                    Data_Msg = {"Equipment_Name": "Adhesive_2",
                                "Tag_Name": "STOP",
                                "Value": Adhesive_2[1],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/3", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Adhesive_2[2] == 1:
                    Data_Msg = {"Equipment_Name": "Adhesive_2",
                                "Tag_Name": "IDLE_ON",
                                "Value": Adhesive_2[2],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/3", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish
                if Adhesive_2[3] == 1:
                    Data_Msg = {"Equipment_Name": "Adhesive_2",
                                "Tag_Name": "IDLE_OFF",
                                "Value": Adhesive_2[3],
                                "Date_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    message_Json = str(json.dumps(Data_Msg))
                    print(message_Json)
                    client.publish("Yamaha/PMS/3", message_Json, qos=2, retain=True)
                    client.on_publish = on_publish

        except Exception as ex:
            logger.error("exception in device connection : " + str(ex))
            print(ex)
            if Converter_2_Status is False:
                Converter_2_Status = Connection.connect()
                print("Converter_2 Connection Status is :" + str(Converter_2_Status))
                time.sleep(1)
            else:
                pass
            
            
if __name__ == "__main__":
    MA8_IP = "192.168.1.100"
    Converter_1_ip = "192.168.1.22"
    Converter_2_ip = "192.168.1.21"
    Port = 502
    logger = logging.getLogger('Yamaha_Publish_logger')
    logger.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logHandler = handlers.TimedRotatingFileHandler('PLC/Yamaha_Pub.log', when='H', interval=24,
                                                   backupCount=5)
    logHandler.setLevel(logging.INFO)
    logHandler.setLevel(logging.DEBUG)
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)

    try:
        client = mqtt.Client("Yamaha_new1", False)
        client.on_message = on_message
        client.connect("192.168.1.32", 1883, 60)
        client.on_publish = on_publish
        client.loop_start()
    except Exception as Ex:
        logger.error("exception in device connection : " + str(Ex))
        print(Ex)
    try:
        Camera_Inspection_Con = ModbusTcpClient(MA8_IP, Port)
        # Camera_Inspection_Status = Camera_Inspection_Con.connect()
        time.sleep(0.2)
        # print("Camera_Inspection Connection Status is :" + str(Camera_Inspection_Status))
        Converter_1 = ModbusTcpClient(Converter_1_ip, Port)
        time.sleep(0.2)
        # Converter_1_Status = Converter_1.connect()
        # print("Converter_1 Connection Status is :" + str(Converter_1_Status))
        Converter_2 = ModbusTcpClient(Converter_2_ip, Port)
        time.sleep(0.2)
        # Converter_2_Status = Converter_2.connect()
        # print("Converter_2 Connection Status is :" + str(Converter_2_Status))
        time.sleep(1)
        connections = [Camera_Inspection_Con, Converter_1, Converter_2]
        print(connections)
    except Exception as Ex:
        logger.error("exception in device connection : " + str(Ex))
        print(Ex)

    try:
        A = threading.Thread(target=Camera_Inspection_Thread, args=(connections[0],))
        A.start()
        print("Camera_Inspection_Thread is alive :" + str(A.is_alive()))
        time.sleep(2)
    except Exception as ex:
        logger.error("exception in device connection : " + str(ex))
        print(ex)
    try:
        C = threading.Thread(target=Converter_1_Thread, args=(connections[1],))
        C.start()
        print("Converter_1_Thread is alive :" + str(C.is_alive()))
        time.sleep(2)
    except Exception as ex:
        logger.error("exception in device connection : " + str(ex))
        print(ex)
    try:
        B = threading.Thread(target=Converter_2_Thread, args=(connections[2],))
        B.start()
        print("Converter_2_Thread is alive :" + str(C.is_alive()))
        time.sleep(2)
    except Exception as ex:
        logger.error("exception in device connection : " + str(ex))
        print(ex)
    while True:

        if not C.is_alive():
            try:
                C = threading.Thread(target=Converter_1_Thread)
                C.start()
                print("Converter_1_Thread is alive :" + str(C.is_alive()))
                time.sleep(2)
            except Exception as ex:
                logger.error("exception in device connection : " + str(ex))
                print(ex)

        if not B.is_alive():
            try:
                B = threading.Thread(target=Converter_2_Thread)
                B.start()
                print("Converter_2_Thread is alive :" + str(C.is_alive()))
                time.sleep(2)
            except Exception as ex:
                logger.error("exception in device connection : " + str(ex))
                print(ex)
            