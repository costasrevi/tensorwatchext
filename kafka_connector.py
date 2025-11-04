import time
import threading
from queue import Queue
import random
import json
import pickle
from typing import Dict
from confluent_kafka import Consumer
from .watcher import Watcher
from probables import CountMinSketch
import logging

# Optional Parsers
try:
    import xmltodict
except ImportError:
    xmltodict = None

try:
    # import avro.schema
    # from avro.io import DatumReader, BinaryDecoder
    # import io
    from fastavro import schemaless_reader
    import io
except ImportError:
    avro = None

try:
    from protobuf_to_dict import protobuf_to_dict
    from google.protobuf import message
except ImportError:
    protobuf_to_dict = None


class kafka_connector(threading.Thread):
    """
    A Kafka consumer that runs in a separate thread to consume messages from a Kafka topic.
    It supports various message formats and integrates with TensorWatch for real-time data visualization.
    """
    def __init__(self, hosts="localhost:9092", topic=None, parsetype=None, avro_schema=None, queue_length=50000,
                 cluster_size=1, consumer_config=None, poll=1.0, auto_offset="earliest", group_id="mygroup", parser_extra=None,
                 decode="utf-8", schema_path=None, protobuf_message=None, random_sampling=None, countmin_width=None,
                 countmin_depth=None, twapi_instance=None, ordering_field=None):
        """
        Initializes the kafka_connector.

        Args:
            hosts (str): Comma-separated list of Kafka brokers.
            topic (str): The Kafka topic to consume from.
            parsetype (str): The format of the messages (e.g., "json", "pickle", "xml", "avro", "protobuf").
            avro_schema (str): The Avro schema for message deserialization.
            queue_length (int): The maximum number of messages to store in the internal queue.
            cluster_size (int): The number of consumer threads to run.
            consumer_config (dict): A dictionary of Kafka consumer configuration settings.
            poll (float): The timeout for polling for new messages from Kafka.
            auto_offset (str): The offset reset policy.
            group_id (str): The consumer group ID.
            parser_extra (str): Extra information for the parser (e.g., Protobuf module).
            decode (str): The encoding to use for decoding messages.
            schema_path (str): The path to the Avro schema file.
            protobuf_message (str): The name of the Protobuf message class.
            random_sampling (int): The percentage of messages to sample (0-100).
            countmin_width (int): The width of the Count-Min Sketch.
            countmin_depth (int): The depth of the Count-Min Sketch.
            twapi_instance: An instance of the TensorWatch API for updating metrics.
            ordering_field (str): Optional message field to maintain global ordering.
        """
        super().__init__()
        self.hosts = hosts or "localhost:9092"
        self.topic = topic
        self.cluster_size = cluster_size
        self.decode = decode
        self.parsetype = parsetype
        self.parser_extra = parser_extra
        self.protobuf_message = protobuf_message
        self.queue_length = queue_length
        self.data = Queue(maxsize=queue_length)
        self.cms = {}  # Count-Min Sketch table
        self.countmin_width = countmin_width
        self.countmin_depth = countmin_depth
        self.random_sampling = random_sampling
        self.poll = poll
        self.consumer_config = consumer_config or {
            "bootstrap.servers": self.hosts,
            "group.id": group_id,
            "auto.offset.reset": auto_offset,
        }
        self._quit = threading.Event()
        self.size = 0
        self.watcher = Watcher()
        self.schema = None
        self.reader = None

        self.twapi_instance = twapi_instance
        self.latencies = []
        self.received_count = 0
        self.last_report_time = time.time()
        self.first_message_sent = False

        # --- NEW: Optional ordering field ---
        self.ordering_field = ordering_field
        self.reordering_buffer = []

        # Load Avro Schema if needed
        if parsetype == "avro" and avro:
            try:
                # self.schema = avro.schema.parse(avro_schema)
                # self.reader = DatumReader(self.schema)
                import fastavro
                if isinstance(avro_schema, str):
                    self.avro_schema = json.loads(avro_schema)
                elif isinstance(avro_schema, dict):
                    self.avro_schema = avro_schema
                self._bytes_io = io.BytesIO()
            except Exception as e:
                logging.error(f"Avro Schema Error: {e}, Avro may not work")
                print(f"Avro Schema Error: {e}, Avro may not work")
                return

        # Load Protobuf if needed
        if parsetype == "protobuf" and protobuf_to_dict:
            try:
                import importlib
                import sys
                if schema_path:
                    sys.path.append(schema_path)
                # Use parser_extra for module name, protobuf_message for class name
                module = importlib.import_module(self.parser_extra)
                self.protobuf_class = getattr(module, self.protobuf_message)
            except Exception as e:
                logging.error(f"Protobuf Import Error: {e}")
                print(f"Protobuf Import Error: {e}")
                self.protobuf_class = None

        self.start()

    def myparser(self, message):
        """
        Parses a message based on the specified format.

        Args:
            message: The message to parse.

        Returns:
            The parsed message, or None if parsing fails.
        """
        try:
            if self.parsetype is None or self.parsetype.lower() == "json":
                return json.loads(message.decode(self.decode))
            elif self.parsetype.lower() == "pickle":
                return pickle.loads(message)
            elif self.parsetype.lower() == "xml" and xmltodict:
                return xmltodict.parse(message.decode(self.decode))["root"]
            elif self.parsetype.lower() == "protobuf" and protobuf_to_dict:
                if self.protobuf_class:
                    dynamic_message = self.protobuf_class() # message is already bytes
                    dynamic_message.ParseFromString(message)
                    return protobuf_to_dict(dynamic_message)
            elif self.parsetype.lower() == "avro" and avro:
                # decoder = BinaryDecoder(io.BytesIO(message))
                self._bytes_io.seek(0)
                self._bytes_io.truncate()
                self._bytes_io.write(message)
                self._bytes_io.seek(0)
                return schemaless_reader(self._bytes_io, self.avro_schema)
                # return self.reader.read(decoder)
        except Exception as e:
            logging.error(f"Parsing Error ({self.parsetype}): {e}")
            print(f"Parsing Error ({self.parsetype}): {e}")
        return None

    def process_message(self, msg):
        """
        Processes a single message from Kafka. This includes parsing, calculating latency,
        and adding the message to the data queue.
        """
        receive_time = time.time()
        try:
            # Apply random sampling if configured
            if self.random_sampling and self.random_sampling > random.randint(0, 100):
                return
            message = msg.value() # Keep as bytes for parsers
            if message is None:
                return
            parsed_message = self.myparser(message)

            # Calculate and record latency if send_time is in the message
            if parsed_message and isinstance(parsed_message, dict) and 'send_time' in parsed_message:
                self.received_count += 1
                if parsed_message['send_time']:
                    send_time = parsed_message['send_time']
                    latency = receive_time - send_time
                    self.latencies.append(latency)
                    parsed_message['latency'] = latency
                parsed_message['receive_time'] = receive_time

            # --- Global reordering based on ordering_field ---
            if self.ordering_field and parsed_message and self.ordering_field in parsed_message:
                self.reordering_buffer.append(parsed_message)
                self.reordering_buffer.sort(key=lambda m: m[self.ordering_field])
                while self.reordering_buffer:
                    m = self.reordering_buffer.pop(0)
                    if not self.first_message_sent and self.twapi_instance:
                        logging.info("First message received, enabling apply button.")
                        self.twapi_instance.enable_apply_button()
                        self.twapi_instance.apply_with_debounce()
                        self.first_message_sent = True
                    if not self.data.full():
                        self.data.put(m, block=False)
                    else:
                        _ = self.data.get()
                        self.data.put(parsed_message, block=False)
            else:
                # no reordering, just put it in the queue
                if parsed_message:
                    if not self.first_message_sent and self.twapi_instance:
                        logging.info("First message received, enabling apply button.")
                        self.twapi_instance.enable_apply_button()
                        self.twapi_instance.apply_with_debounce()
                        self.first_message_sent = True
                    if not self.data.full():
                        self.data.put(parsed_message, block=False)
                    else:
                        _ = self.data.get()
                        self.data.put(parsed_message, block=False)

            # Update Count-Min Sketch if configured
            if isinstance(parsed_message, dict) and self.countmin_width and self.countmin_depth:
                for key, value in parsed_message.items():
                    self.cms.setdefault(key, CountMinSketch(width=self.countmin_width, depth=self.countmin_depth))
                    self.cms[key].add(str(value))

            self.size += 1
        except Exception as e:
            logging.error(f"Message Processing Error: {e}, Message: {message}")
            print(f"Message Processing Error: {e}, Message: {message}")

    def consumer_loop(self):
        """
        The main loop for the Kafka consumer. It polls for messages, accumulates
        them in an adaptive batch, and processes them. Handles parsing, CMS,
        latency, and TensorWatch observation.
        """
        logging.info(f"Starting consumer loop for topic '{self.topic}'")
        consumer = Consumer(self.consumer_config)
        consumer.subscribe([self.topic])

        batch = []
        while not self._quit.is_set():
            msg = consumer.poll(self.poll)
            if msg and not msg.error():
                batch.append(msg)
                
                # --- Adaptive batch size based on queue fill ratio ---
                fill_ratio = self.data.qsize() / self.queue_length
                if fill_ratio < 0.5:
                    target_batch_size = 50  # small batch
                elif fill_ratio < 0.8:
                    target_batch_size = 200  # medium batch
                else:
                    target_batch_size = 500  # large batch to catch up
                
                # Process batch if threshold reached
                if len(batch) >= target_batch_size:
                    for m in batch:
                        self.process_message(m)
                    batch.clear()

            elif msg and msg.error():
                logging.error(f"Kafka Error: {msg.error()}")
                print(f"Kafka Error: {msg.error()}")

            # Optional: flush any remaining batch if queue is low
            if batch and self.data.qsize() < 10:
                for m in batch:
                    self.process_message(m)
                batch.clear()

        # Final flush before exiting
        for m in batch:
            self.process_message(m)
        consumer.close()
        logging.info("Consumer loop stopped")


    def run(self):
        """
        Starts the consumer threads and the main watcher loop.
        """
        logging.info(f"Starting {self.cluster_size} consumer threads")
        threads = [threading.Thread(target=self.consumer_loop, daemon=True) for _ in range(self.cluster_size)]
        for thread in threads:
            thread.start()

        while not self._quit.is_set():
            # Observe the data queue with TensorWatch
            if not self.data.empty():
                self.watcher.observe(data=list(self.data.queue), size=self.size, cms=self.cms)

            # --- BENCHMARK REPORTING ---
            current_time = time.time()
            if current_time - self.last_report_time > 5.0:  # Report every 5 seconds
                if self.latencies:
                    avg_latency = sum(self.latencies) / len(self.latencies)
                    max_latency = max(self.latencies)
                    min_latency = min(self.latencies)

                    time_since_last_report = current_time - self.last_report_time
                    throughput = self.received_count / time_since_last_report if time_since_last_report > 0 else 0

                    stats_str = (f"Recv Throughput: {throughput:.2f} msgs/s | "
                                 f"Send-Recv Latency (ms): "
                                 f"Avg: {avg_latency*1000:.2f}, "
                                 f"Min: {min_latency*1000:.2f}, "
                                 f"Max: {max_latency*1000:.2f}")
                    logging.info(f"Benchmark stats: {stats_str}")
                    print(stats_str)

                    # Update the TensorWatch API with the latest metrics
                    if self.twapi_instance:
                        self.twapi_instance.update_metrics(stats_str)

                    # Reset stats for the next interval
                    self.latencies = []
                    self.received_count = 0
                self.last_report_time = current_time

            time.sleep(0.4)
