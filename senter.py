from pykafka import KafkaClient
import re
from datetime import datetime
import os
import json
import pickle

import avro.schema
import io
from avro.io import DatumReader, DatumWriter, BinaryDecoder, BinaryEncoder

import thrift

import sys

import random
client = KafkaClient(hosts="127.0.0.1:9093")
# # Read the files and parse them

# topic = client.topics['my.test']
# setup once
# client = KafkaClient(hosts="127.0.0.1:9093", use_greenlets=True)
topic = client.topics['newtopicpart5']
producer = topic.get_sync_producer()

test_schema = '''
{
"namespace": "example.avro",
 "type": "record",
 "name": "Type",
 "fields": [
     {"name": "Date", "type": "string"},
     {"name": "Request",  "type": ["string", "null"]},
	 {"name": "Rand",  "type": ["int", "null"]},
     {"name": "Extra", "type": ["string", "null"]}
 ]
}
'''
# message DowJonesIndex {
#     int64 quarter = 1;
#     string stock=2;
#     string date=3;
#     string open=4;
#     string high=5;
#     string low=6;
#     string close=7;
#     int64 volume=8;
#     string percent_change_price=9;
#     string percent_change_volume_over_last_wk=10;
#     string previous_weeks_volume=11;
#     string next_weeks_open=12;
#     string next_weeks_close=13;
#     double percent_change_next_weeks_price=14;
#     int64 days_to_next_dividend=15;
#     double percent_return_next_dividend=16;
# }


parse_type = "json"


# def get_pattern_files(root_path, pattern):
#     all_files = []
#     print("get_pattern_files")
#     list_dirs = os.walk(root_path)
#     print(list_dirs)
#     for root, dirs, files in list_dirs:
#         for f in fnmatch.filter(files, pattern):
#             all_files.append(os.path.join(root,f))
#             print(all_files)
#     return all_files


# all_files = get_pattern_files(root_path, pattern)

def main():
    """
    :param producer: pykafka producer
    :param key: key to decide partition
    :param message: json serializable object to send
    :return:
    """
    # pattern = '*.log'
    # root_path = './output'
    # all_files = get_pattern_files(root_path, pattern)
    all_files = []
    # G:\OneDrive\worldcup98-dataset-master\output\wc_day11_1.log   C:\Users\costa\Nextcloud\sxolh\twext2\
    all_files.append("C://Users//costa//OneDrive//worldcup98-dataset-master//output//wc_day11_1.log")
    all_files.append("C://Users//costa//OneDrive//worldcup98-dataset-master//output//wc_day12_1.log")
    all_files.append("C://Users//costa//OneDrive//worldcup98-dataset-master//output//wc_day13_1.log")
    all_files.append("C://Users//costa//OneDrive//worldcup98-dataset-master//output//wc_day14_1.log")
    all_files.append("C://Users//costa//OneDrive//worldcup98-dataset-master//output//wc_day15_1.log")
    with topic.get_producer() as producer:#use_rdkafka=True
        print("get_producer")
        print(all_files)
        for input_file in all_files:
            print("input_file")
            with open(input_file, 'r', encoding='ISO-8859-1') as file_handle:
                # print(input_file, ' --> ', output_file)

                for line in file_handle:
                    # temp = []
                    try:
                        temp = {"Date": "", "Request": "", "Extra": ""}
                        time_str = re.search("\[.*\]", line)
                        time_str = time_str.group()[1:-1]
                        temp["Date"] = (time_str.split(' ')[0])
                        str2 = line.split('"')
                        test = str2[1].split(' ')
                        temp["Request"] = (test[0])
                        temp["Extra"] = (test[1].split('/')[1])
                        if parse_type == "protobuf":
                            # converted_content=???
                            converted_string = converted_content.SerializeToString()
                            converted_bytes = converted_content.__bytes__() 
                            converted_bytes = bytes(converted_content)
                        if parse_type == "thrift":
                                transportOut = TTransport.TMemoryBuffer()
                                protocolOut = protocol_type(transportOut)
                                asd1=write(protocolOut)
                        if parse_type == "avro":
                            schema = avro.schema.parse(test_schema)
                            writer = avro.io.DatumWriter(schema)
                            bytes_writer = io.BytesIO()
                            encoder = avro.io.BinaryEncoder(bytes_writer)
                            asd = random.randint(0,9)
                            writer.write({"Date":str(temp["Date"]), "Rand": asd, "Request": str(temp["Request"]), "Extra": str(temp["Extra"])},encoder)
                            producer.produce(bytes_writer.getvalue())
                        # print(bytes_writer.getvalue().encode('utf-8'))
                        if parse_type == "json":
                            data = json.dumps(temp)
                            producer.produce(data.encode('utf-8'))
                    except:
                        pass
                    # format: 30/Apr/1998:21:30:17
                    # datetime_object = datetime.strptime(time_str, '%d/%b/%Y:%H:%M:%S')
                    # print("before produce")
                    # time_objects.append(time_str)
                    # try:
                    # start = time.time()
                    # data = bytes(time_objects)
                    # temp=temp.append(time_str),test[0],test2    
                    # time_objects.append()
                    # print(temp)

                    # time_objects = []
                    # data = pickle.dumps(time_objects )
                    # producer.produce(data)
                    # producer.produce(time_objects, partition_key='{}'.format(key))

                    # logger.info(u'Time take to push to Kafka: {}'.format(time.time() - start))
                    # except Exception as e:
                    # logger.exception(e)
                    # pass # for at least once delivery you will need to catch network errors and retry
                    # time_objects.append(datetime_object)

                    # producer.produce(time_objects)
                    # print("after produce")    
        print("end")
        sys.exit()

    # data = json.dumps(message)


if __name__ == "__main__":
    main()
