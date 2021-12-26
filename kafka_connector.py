from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from .watcher import Watcher

import threading
from queue import Queue

#imported parsers here that need no install
import json
import pickle
import xml.etree.ElementTree as ET
import xmltodict as xmltodict
# from lxml import etree
from datetime import datetime
import time
from typing import Dict

def get_ioloop():
    import IPython, zmq
    ipython = IPython.get_ipython()
    if ipython and hasattr(ipython, 'kernel'):
        return zmq.eventloop.ioloop.IOLoop.instance()


#The IOloop is shared
ioloop = get_ioloop()

class kafka_connector(threading.Thread):
                
    def quit(self):
        self._quit.set()        

    def __init__(self, hosts:str="localhost:9092", topic:str=None, parsetype:str=None, parser_extra:str=None, queue_length:int=None, cluster_size:int=1, 
    consumer_config:Dict=None, poll:float=1.0 ,auto_offset:str="earliest", group_id:str="mygroup", decode:str="utf-8"):
        super().__init__()
        self.hosts = hosts
        self.topic = topic
        self.cluster_size = cluster_size
        self.size = 0
        self.decode = decode
        # self.pykfka_setting=pykfka_setting
        self.kafka_thread = None
        self.parsetype = parsetype
        # self.schema=schema
        self.parser_extra = parser_extra
        self.queue_length = queue_length
        if self.queue_length is None:
            self.data=Queue(maxsize=50000)
        else:
            self.data=Queue(maxsize=self.queue_length)
        #confluent parameters
        self.consumer_config = consumer_config
        self.poll = poll
        self.auto_offset = auto_offset
        self.group_id = group_id
        self._quit = threading.Event()
        # self._quit = threading.Event()
        #pykfka
        #to skip decoders from needed to be install 
        if self.parsetype is  None:
            pass
        elif self.parsetype.lower()=='thrift' :   
            try:
                import thrift
                from thrift.protocol import TBinaryProtocol
                from thrift.transport import TTransport
            except: 
                print("thrift not installed")
                return
        elif self.parsetype.lower()=='avro' :
            try:
                import avro.schema
                # import io
                from avro.io import DatumReader
                schema = avro.schema.parse(parser_extra)
                self.reader = DatumReader(schema)
            # except: 
            #     print("avro not installed")
            #     return
            # try:
            #     self.reader = DatumReader(json.loads(parser_extra))
            except: 
                print("avro second error not installed"+ json.loads(parser_extra))
                return
        # elif self.parsetype.lower()=='protobuf' :
        #     try:
        #         import ParseFromString
        #     except: 
        #         print("ParseFromString not installed")
        #         return
        # time.sleep(0.2)
        self.start()

    #trying to add deferent libraries for deserializing the kafka messages
    def myparser(self,message):#,parsetype=None,parser_extra=None):
        if self.parsetype is None or self.parsetype.lower()=='json' :
            return json.loads(message)
        elif self.parsetype.lower()=='pickle' :
            return pickle.loads(message)
        # elif self.parsetype.lower()=='thrift' :
            # transportIn = TTransport.TMemoryBuffer(message)
            # return TBinaryProtocol.TBinaryProtocol(transportIn)
            # return TDeserializer.deserialize(self.parser_extra, message)
        elif self.parsetype.lower()=='xml' :
            return xmltodict.parse(message,encoding=self.decode)
            xml = bytes(bytearray(message, encoding = self.decode))
            return ET.parse(xml)
        elif self.parsetype.lower()=='protobuf' :
            # s = str(message, 'ascii')
            s = str(message, self.decode)
            # return ParseFromString(s)
            # import base64
            # s = base64.b64decode(data).decode('utf-8')
            # message.ParseFromString(s)
            # transportIn = TTransport.TMemoryBuffer(message)
            # return TBinaryProtocol.TBinaryProtocol(transportIn)
        elif self.parsetype.lower()=='avro' :
            import io
            from avro.io import BinaryDecoder
            message_bytes = io.BytesIO(message)
            decoder = BinaryDecoder(message_bytes)
            event_dict = self.reader.read(decoder)
            # print(event_dict)
            # bytes_reader = io.BytesIO(raw_bytes)
            # decoder = avro.io.BinaryDecoder(bytes_reader)
            # reader = avro.io.DatumReader(schema)
            # decoded_data = reader.read(decoder)
            return event_dict
        return 'error:unkown type of parsing'

    def consumer(self):
        print("consumer start")
        if self.consumer_config is None:
            c = Consumer({
            'bootstrap.servers': self.hosts,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset
            })
        else:  
            c = Consumer(self.consumer_config)
        c.subscribe([self.topic])
        while True:
            msg = c.poll(self.poll)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            temp=self.myparser(format(msg.value().decode(self.decode)))#,parsetype)
            # print(temp)
            # temp["Date"]= datetime.strptime(temp["Date"], '%d/%b/%Y:%H:%M:%S')#just for our testing will be removed later
            # temp["recDate"]=datetime.now() #also that perhaps ?
            if self.data.full():
                self.data.get()
            self.data.put(temp)
            self.size+=1

    def run(self):
        # kafkaext='confluent'
        # kafkaext='pykfka'
        w = Watcher()
        #queue_length is the maximum messages that will be kept in memory
        if self.cluster_size==1:
            if self.consumer_config is None:
                c = Consumer({
                'bootstrap.servers': self.hosts,
                'group.id': self.group_id,
                'auto.offset.reset': self.auto_offset
                })
            else:  
                c = Consumer(self.consumer_config)
            c.subscribe([self.topic])
            while True:
                msg = c.poll(self.poll)
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue
                temp=self.myparser(format(msg.value().decode(self.decode)))#,parsetype)
                # temp["Date"]= datetime.strptime(temp["Date"], '%d/%b/%Y:%H:%M:%S')#just for our testing will be removed later
                # temp["recDate"]=datetime.now() #also that perhaps ?
                print(temp)
                print(type(temp))
                if self.data.full():
                    self.data.get()
                self.data.put(temp)
                self.size+=1
                w.observe(data=list(self.data.queue),size=self.size)
        else:
            thread=[]
            for x in range(self.cluster_size):
                thread.append(threading.Thread(target = self.consumer, args=()))
                thread[x].daemon = True
                thread[x].start()
            while True:
                w.observe(data=list(self.data.queue),size=self.size)
                time.sleep(0.5)

