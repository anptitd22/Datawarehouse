import logging
import time

from dags.monitoring.build_logging import setup_logging
from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringSerializer

#set up kafka
KAFKA_BROKER = 'kafka:9092'
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'

BATCH_SIZE = 500
CONSUME_TIMEOUT = 5.0
RETRY_LIMIT = 3
RETRY_DELAY = 2  # seconds between retries

setup_logging("kafka.log")
logger = logging.getLogger(__name__)

#producer protobuf
def get_producer(Object_Message):
    protobuf_serializer = ProtobufSerializer(
        Object_Message,
        SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    )
    return SerializingProducer({
        'bootstrap.servers': KAFKA_BROKER,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': lambda v, ctx: protobuf_serializer(v, ctx),
        'enable.idempotence': True,
        'acks': 'all',
        'batch.size': 16384,
        'linger.ms': 300,
        'compression.type': 'lz4'
    })

#consumer protobuf
def get_consumer(Object_Message, GROUP_ID):
    protobuf_deserializer = ProtobufDeserializer(
        message_type=Object_Message,
        conf={"schema.registry.url": SCHEMA_REGISTRY_URL}
    )

    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'fetch.min.bytes': 1024,
        'session.timeout.ms': 6000,
        'max.partition.fetch.bytes': 65536,
    }

    consumer = DeserializingConsumer({
        **consumer_conf,
        'value.deserializer': lambda data, ctx: protobuf_deserializer(data, ctx),
    })
    return consumer

def delivery_report(err, msg):
    if err is not None:
        logger.info(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def ensure_topic_exists(topic_name, num_partitions=5, replication_factor=1, retries=RETRY_LIMIT, delay=RETRY_DELAY):
    admin_client = AdminClient({'bootstrap.servers': 'kafka:9092'})

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Thử lần {attempt}/{retries} kiểm tra topic '{topic_name}'...")
            metadata = admin_client.list_topics(timeout=5)

            if topic_name in metadata.topics:
                logger.info(f"Topic '{topic_name}' đã tồn tại.")
                return

            logger.info(f"Đang tạo topic '{topic_name}' với {num_partitions} partitions...")
            topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            fs = admin_client.create_topics([topic])

            for t, f in fs.items():
                f.result()  # Block until complete or raise exception
                logger.info(f"Đã tạo topic: {t}")
            return
        except Exception as e:
            logger.warning(f"⚠Lỗi khi kiểm tra/tạo topic '{topic_name}': {e}")

            if attempt < retries:
                logger.info(f"Đợi {delay} giây rồi thử lại...")
                time.sleep(delay)
            else:
                logger.error(f"Thử {retries} lần nhưng vẫn lỗi khi kiểm tra/tạo topic '{topic_name}'.")
                raise e  # Nếu hết retries → raise lỗi cuối cùng


