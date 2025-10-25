from confluent_kafka import Consumer
# from confluent_kafka.admin import AdminClient

from .watcher import Watcher

import threading
from queue import Queue

#imported parsers here that need no install
import json
import pickle
#try to import parsers that we may need without the need to have them all installed
try:
    import xmltodict as xmltodict   
except:
    pass
try:
    import avro.schema
    from avro.io import DatumReader
    import io
    from avro.io import BinaryDecoder
except:
    pass
try:
    from protobuf_to_dict import protobuf_to_dict
except:
    pass
from datetime import datetime       #only used for testing atm
import time
import random
from typing import Dict #perhaps can be remove from here and init too
from probables import (CountMinSketch)

#Used to get thread from ipython for main to run on background
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
    #Main init to get inputs from user and initiate them
    def __init__(self, hosts:str="localhost:9092", topic:str=None, parsetype:str=None, avro_scema:str=None, queue_length:int=None, cluster_size:int=1, 
    consumer_config:Dict=None, poll:float=1.0 ,auto_offset:str="earliest", group_id:str="mygroup", decode:str="utf-8", scema_path:str=None,
     probuf_message:str=None, random_sampling:int=None, countmin_width:int=None, countmin_depth:int=None, protobuf_message:str=None):
        super().__init__()
        #basic kafka confluent settings
        self.hosts = hosts
        self.topic = topic
        #used for multithreading
        self.cluster_size = cluster_size
        self.kafka_thread = None
        self.size = 0                                   #just for benchmark
        # extra inputs for parsers/decoders
        self.scema_path = scema_path
        self.decode = decode
        self.avro_scema = avro_scema
        self.protobuf_message = protobuf_message
        self.parsetype = parsetype
        #memory optimazation limiting the number of stored data
        self.cms={}
        self.countmin_depth = countmin_depth
        self.countmin_width = countmin_width
        self.random_sampling = random_sampling
        #queue_length is the maximum messages that will be kept in memory
        self.queue_length = queue_length
        if self.queue_length is None:
            self.data=Queue(maxsize=50000)
        else:
            self.data=Queue(maxsize=self.queue_length)
        #confluent extra parameters
        self.consumer_config = consumer_config
        self.poll = poll
        self.auto_offset = auto_offset
        self.group_id = group_id
        self._quit = threading.Event()
        # self.starttime = time.time()                    #benchmark testing
        #Try to import needed decoders that need input from user
        if self.parsetype is  None:
            pass
        elif self.parsetype.lower()=='avro' :
            try:
                schema = avro.schema.parse(avro_scema)
                self.reader = DatumReader(schema)
            except: 
                print("avro schema error or avro not installed"+ json.loads(avro_scema))
                return
        elif self.parsetype.lower()=='protobuf' :
            try:
                import sys
                import importlib
                sys.path.append(scema_path)#scema_path change them
                mymodule = importlib.import_module(protobuf_message)
                method_to_call = getattr(mymodule, probuf_message)
                self.mymodule = method_to_call()
            except: 
                print("Error importing protobuf")
        # print("before start:  "+str(time.time()-self.starttime))    #benchmark testing
        self.start()

    #trying to add deferent libraries for deserializing the kafka messages
    def myparser(self,message):
        if self.parsetype is None or self.parsetype.lower()=='json' :#or self.parsetype.lower()=='xml' :
            return json.loads(message)
        elif self.parsetype.lower()=='pickle' :
            return pickle.loads(message)
        elif self.parsetype.lower()=='xml' :
            try:
                xml = xmltodict.parse(message)
                return xml["root"]
            except Exception: 
                print('xmltodict not installed or Exception occured : ' + message)
        elif self.parsetype.lower()=='protobuf' :
            temp_message=self.mymodule
            temp_message.ParseFromString(message)
            my_message_dict = protobuf_to_dict(temp_message)
            return my_message_dict
        elif self.parsetype.lower()=='avro' :
            try:
                message_bytes = io.BytesIO(message)
                decoder = BinaryDecoder(message_bytes)
                event_dict = self.reader.read(decoder)
                return event_dict
            except:
                print("Avro error ,perhpas avro not installed")
                return
        return 'error:unkown type of parsing'
    
    #consumer loop for multithreading
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
            if self.random_sampling is not None and self.random_sampling>random.randint(0,100):
                    pass
            else:   
                if self.parsetype.lower()=="protobuf":
                    temp=self.myparser(msg.value())
                else:
                    temp=self.myparser(format(msg.value().decode(self.decode)))
                if self.data.full():
                    self.data.get()  
                self.data.put(temp)
            self.size+=1
            try:
                if type(temp) is dict and self.countmin_depth is not None and self.countmin_width is not None:
                    # print("it is dict")
                    for key in temp:
                        # print(key)
                        if key not in self.cms.keys():
                            try:
                                self.cms[key] = CountMinSketch(width=self.countmin_width, depth=self.countmin_depth)
                            except:
                                print("self.cms[key] is None")
                        try:
                            self.cms[key].add(str(temp[key]))
                        except:
                            print("self.cms[key].add(str(temp[key]))")
                elif type(temp) is list and self.countmin_depth is not None and self.countmin_width is not None:
                    for key in temp:
                        if self.cms[key] is None:
                            self.cms[str(key)] = CountMinSketch(width=self.countmin_width, depth=self.countmin_depth)
                        self.cms[str(key)].add(str(temp[str(key)]))                        
            except:
                print("cms assigment error")

    #Starts after initiation and runs the basic loop or initiated the other threads
    def run(self):
        w = Watcher()
        # print("running after watcher import:  "+str(time.time()-self.starttime))    #benchmark testing
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
            # print("after subcriber:  "+str(time.time()-self.starttime))    #benchmark testing
            while True:
                # print("before poll:  "+str(time.time()-self.starttime)+"    poll type  :    "+str(type(self.poll)) )
                msg = c.poll(self.poll)
                if msg is None:
                    # print("its message nononone:  "+str(time.time()-self.starttime))    #benchmark testing
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue
                # print("its message:  "+str(time.time()-self.starttime))    #benchmark testing
                if self.random_sampling is not None and self.random_sampling>random.randint(0,100):
                    pass
                else:
                    if self.parsetype.lower()=="protobuf" or self.parsetype.lower()=="pickle":
                        temp=self.myparser(msg.value())
                    else:
                        temp=self.myparser(format(msg.value().decode(self.decode)))
                # temp["Date"]= datetime.strptime(temp["Date"], '%d/%b/%Y:%H:%M:%S')#just for our testing will be removed later
                # temp["recDate"]=datetime.now() #also that perhaps ?
                # print("after message parsd:  "+str(time.time()-self.starttime))    #benchmark testing
                    if self.data.full():
                        self.data.get()
                    self.data.put(temp)
                    #testing count mean
                    try:
                        if type(temp) is dict and self.countmin_depth is not None and self.countmin_width is not None:
                            # print("it is dict")
                            for key in temp:
                                # print(key)
                                if key not in self.cms.keys():
                                    try:
                                        self.cms[key] = CountMinSketch(width=self.countmin_width, depth=self.countmin_depth)
                                    except:
                                        print("self.cms[key] is None")
                                try:
                                    self.cms[key].add(str(temp[key]))           #add.lower() to make every string lower caps ?????????
                                except:
                                    print("self.cms[key].add(str(temp[key]))")
                        elif type(temp) is list and self.countmin_depth is not None and self.countmin_width is not None:
                            for key in temp:
                                if self.cms[key] is None:
                                    self.cms[str(key)] = CountMinSketch(width=self.countmin_width, depth=self.countmin_depth)
                                self.cms[str(key)].add(str(temp[str(key)]))                        
                    except:
                        print("cms assigment error")
                self.size+=1
                w.observe(data=list(self.data.queue), size=self.size, cms=self.cms)#,cmsDate=self.cmsDate,cmsRequest=self.cmsRequest,cmsExtra=self.cmsExtra,cmsRand=self.cmsRand
                # w.observe(data=list(self.data.queue),size=self.size,cmsRand=self.cmsRand)
        else:
            thread=[]
            for x in range(self.cluster_size):
                thread.append(threading.Thread(target = self.consumer, args=()))
                thread[x].daemon = True
                thread[x].start()
            while True:
                w.observe(data=list(self.data.queue), size=self.size, cms=self.cms)
                time.sleep(0.4)