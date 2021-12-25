from pykafka import KafkaClient
from pykafka.common import OffsetType
from .watcher import Watcher

import threading
from queue import Queue

#imported parsers here that need no install
import json
import pickle
import xml.etree.ElementTree as ET
# from lxml import etree
from datetime import datetime
import time

def get_ioloop():
    import IPython, zmq
    ipython = IPython.get_ipython()
    if ipython and hasattr(ipython, 'kernel'):
        return zmq.eventloop.ioloop.IOLoop.instance()


#The IOloop is shared
ioloop = get_ioloop()

# def run_progress(hosts:str=None, topic:str=None, parsetype:str=None, parser_extra:str=None, queue_length:int=None):
#         thread = kafka_contector(hosts,topic,parsetype,parser_extra,queue_length)
#         print("test")
#         return
#         # return thread

# from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable
# # this illustrates consumer error catching;
# consumer = topic.get_simple_consumer()
# try:
#     consumer.consume()
# except (SocketDisconnectedError) as e:
#     consumer = topic.get_simple_consumer()
#     # use either the above method or the following:
#     consumer.stop()
#     consumer.start()

class pykafka_connector(threading.Thread):
                
    def quit(self):
        self._quit.set()        

    def __init__(self, hosts:str=None, topic:str=None, parsetype:str=None, parser_extra:str=None, queue_length:int=None, cluster_size:int=1,
    #pykfka settings        
    cluster:str=None, consumer_group:str="mygroup", partitions:str=None,
    fetch_message_max_bytes:int=1024 * 1024, num_consumer_fetchers:int=1, auto_commit_enable:bool=False,
    auto_commit_interval_ms:int=60 * 1000, queued_max_messages:int=2000, fetch_min_bytes:int=1,  fetch_error_backoff_ms:int=500, fetch_wait_max_ms:int=100,
    offsets_channel_backoff_ms:int=1000, offsets_commit_max_retries:int=5, auto_offset_reset:OffsetType=OffsetType.EARLIEST, consumer_timeout_ms:int=-1, auto_start:bool=True,
    reset_offset_on_start:bool=False, compacted_topic:bool=False, generation_id:int=-1, consumer_id:bool=b'',  reset_offset_on_fetch:bool=True, decode:str="utf-8"):#deserializer:function=None,
        super().__init__()
        self.hosts = hosts
        self.topic = topic
        self.cluster_size=cluster_size
        self.decode = decode
        self.size=0
        self.kafka_thread = None
        self.parsetype = parsetype
        # self.schema=schema
        self.parser_extra = parser_extra
        self.queue_length = queue_length
        if self.queue_length is None:
            self.data=Queue(maxsize=50000)
        else:
            self.data=Queue(maxsize=self.queue_length)
        self._quit = threading.Event()
        # self._quit = threading.Event()
        #pykfka
        self.cluster, self.consumer_group, self.partitions= cluster, consumer_group, partitions
        self.fetch_message_max_bytes, self.num_consumer_fetchers = \
            fetch_message_max_bytes, num_consumer_fetchers
        self.auto_commit_enable, self.auto_commit_interval_ms, self.queued_max_messages, self.fetch_min_bytes, self.fetch_error_backoff_ms = \
            auto_commit_enable, auto_commit_interval_ms, queued_max_messages, fetch_min_bytes, fetch_error_backoff_ms
        self.fetch_wait_max_ms, self.offsets_channel_backoff_ms, self.offsets_commit_max_retries, self.auto_offset_reset, self.consumer_timeout_ms = \
            fetch_wait_max_ms, offsets_channel_backoff_ms, offsets_commit_max_retries, auto_offset_reset, consumer_timeout_ms
        self.auto_start, self.reset_offset_on_start, self.compacted_topic, self.generation_id, self.consumer_id, self.reset_offset_on_fetch= \
            auto_start, reset_offset_on_start, compacted_topic, generation_id, consumer_id, reset_offset_on_fetch
        # self.deserializer = deserializer
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
                from avro.io import DatumReader#,
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
        if self.hosts is None:
            client = KafkaClient(hosts="127.0.0.1:9092")
        else:
            print("else")
            client = KafkaClient(hosts=self.hosts)
        topic = client.topics[self.topic]
        consumer = topic.BalancedConsumer(consumer_group=self.consumer_group, fetch_message_max_bytes=self.fetch_message_max_bytes,
        num_consumer_fetchers=self.num_consumer_fetchers, auto_commit_enable=self.auto_commit_enable, auto_commit_interval_ms=self.auto_commit_interval_ms, queued_max_messages=self.queued_max_messages, 
        fetch_min_bytes=self.fetch_min_bytes, fetch_error_backoff_ms=self.fetch_error_backoff_ms, fetch_wait_max_ms=self.fetch_wait_max_ms, offsets_channel_backoff_ms=self.offsets_channel_backoff_ms, 
        offsets_commit_max_retries=self.offsets_commit_max_retries, auto_offset_reset=self.auto_offset_reset, consumer_timeout_ms=self.consumer_timeout_ms, auto_start=self.auto_start,
        reset_offset_on_start=self.reset_offset_on_start, compacted_topic=self.compacted_topic, generation_id=self.generation_id, consumer_id=self.consumer_id, reset_offset_on_fetch=self.reset_offset_on_fetch )
        for message in consumer:
            if message is not None:
                temp=self.myparser(message.value)#,parsetype)
                # print(temp)
                temp["Date"]= datetime.strptime(temp["Date"], '%d/%b/%Y:%H:%M:%S')#just for our testing will be removed later
                temp["recDate"]=datetime.now() #also that perhaps ?
                self.data.put(temp)
                self.size+=1

    def run(self):
        # kafkaext='confluent'
        # kafkaext='pykfka'
        w = Watcher()
        #queue_length is the maximum messages that will be kept in memory
        if self.cluster_size==1:
            if self.hosts is None:
                client = KafkaClient(hosts="127.0.0.1:9092")
            else:
                print("else")
                client = KafkaClient(hosts=self.hosts)
            topic = client.topics[self.topic]
            consumer = topic.get_simple_consumer(fetch_message_max_bytes=self.fetch_message_max_bytes,
            num_consumer_fetchers=self.num_consumer_fetchers, auto_commit_enable=self.auto_commit_enable, auto_commit_interval_ms=self.auto_commit_interval_ms, queued_max_messages=self.queued_max_messages, 
            fetch_min_bytes=self.fetch_min_bytes, fetch_error_backoff_ms=self.fetch_error_backoff_ms, fetch_wait_max_ms=self.fetch_wait_max_ms, offsets_channel_backoff_ms=self.offsets_channel_backoff_ms, 
            offsets_commit_max_retries=self.offsets_commit_max_retries, auto_offset_reset=self.auto_offset_reset, consumer_timeout_ms=self.consumer_timeout_ms, auto_start=self.auto_start,
            reset_offset_on_start=self.reset_offset_on_start, compacted_topic=self.compacted_topic, generation_id=self.generation_id, consumer_id=self.consumer_id, reset_offset_on_fetch=self.reset_offset_on_fetch )
            for message in consumer:
                if message is not None:
                    temp=self.myparser(message.value)#,parsetype)
                    # print(temp)
                    temp["Date"]= datetime.strptime(temp["Date"], '%d/%b/%Y:%H:%M:%S')#just for our testing will be removed later
                    temp["recDate"]=datetime.now() #also that perhaps ?
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


        
        # elif kafkaext=='confluent':

                    # self.size+=1
                    
                # print('Received message: {}'.format(msg.value().decode('utf-8')))
        # consumer = topic.get_simple_consumer()
        # if self.consumer_group is not None:
        #     consumer = topic.get_simple_consumer(topic=self.topic, cluster=self.cluster, consumer_group=self.consumer_group, partitions=self.partitions, fetch_message_max_bytes=self.fetch_message_max_bytes,
        #     num_consumer_fetchers=self.num_consumer_fetchers, auto_commit_enable=self.auto_commit_enable, auto_commit_interval_ms=self.auto_commit_interval_ms, queued_max_messages=self.queued_max_messages, 
        #     fetch_min_bytes=self.fetch_min_bytes, fetch_error_backoff_ms=self.fetch_error_backoff_ms, fetch_wait_max_ms=self.fetch_wait_max_ms, offsets_channel_backoff_ms=self.offsets_channel_backoff_ms, 
        #     offsets_commit_max_retries=self.offsets_commit_max_retries, auto_offset_reset=self.auto_offset_reset, consumer_timeout_ms=self.consumer_timeout_ms, auto_start=self.auto_start,
        #     reset_offset_on_start=self.reset_offset_on_start, compacted_topic=self.compacted_topic, generation_id=self.generation_id, consumer_id=self.consumer_id, reset_offset_on_fetch=self.reset_offset_on_fetch )#deserializer=self.deserializer,
        # else:
        #deserializer=self.deserializer,  cluster=self.cluster, partitions=self.partitions,
        # consumer = topic.get_simple_consumer(auto_offset_reset=OffsetType.LATEST,reset_offset_on_start=True,fetch_wait_max_ms =50)
