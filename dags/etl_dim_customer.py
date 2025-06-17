import time
import os
import uuid
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when
from pyspark.sql.types import IntegerType, DoubleType
from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from google.protobuf import descriptor_pool, message_factory
from google.protobuf.message_factory import GetMessageClass
from db_connections import get_pg_conn, get_sqlserver_conn
from proto.generated import customer_pb2

KAFKA_BROKER = 'kafka:9092'
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
TOPIC_NAME = 'dim_customer'

# Nếu bạn đã có DESCRIPTOR từ file .proto generated
descriptor = customer_pb2.DESCRIPTOR.message_types_by_name['Customer']

# Tạo dynamic message class từ descriptor
CustomerMessage = GetMessageClass(descriptor)

def get_producer():
    protobuf_serializer = ProtobufSerializer(
        CustomerMessage,
        SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    )
    return SerializingProducer({
        'bootstrap.servers': KAFKA_BROKER,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': lambda v, ctx: protobuf_serializer(v, ctx),
        'enable.idempotence': True,
        'acks': 'all',
        'batch.size': 32768,
        'linger.ms': 100,
    })

def get_spark():
    return SparkSession.builder \
        .appName("ETLTransformCustomer") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages",
               "org.postgresql:postgresql:42.7.1,"
               "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.driver.extraJavaOptions",
               "--add-modules=jdk.incubator.vector "
               "-Djava.net.preferIPv4Stack=true "
               "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.executor.extraJavaOptions",
               "--add-modules=jdk.incubator.vector "
               "-Djava.net.preferIPv4Stack=true "
               "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.executor.extraJavaOptions", "-Dorg.postgresql.forcebinary=true") \
        .config("spark.driver.extraJavaOptions", "-Dorg.postgresql.forcebinary=true") \
        .getOrCreate()

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def ensure_topic_exists(topic_name, num_partitions=5, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': 'kafka:9092'})
    metadata = admin_client.list_topics(timeout=5)
    if topic_name not in metadata.topics:
        topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        fs = admin_client.create_topics([topic])
        for t, f in fs.items():
            try:
                f.result()
                print(f"Đã tạo topic: {t}")
            except Exception as e:
                print(f"Không thể tạo topic {t}: {e}")
    else:
        print(f"Topic '{topic_name}' đã tồn tại.")

# def extract_customer():
#     spark = get_spark()
#     query = """
#             SELECT
#                 c.id AS client_id,
#                 c.name as name,
#                 c.email as email,
#                 c.phone as phone,
#                 c.address as address,
#                 CAST(COALESCE(SUM(o.total_amount), 0) AS DOUBLE PRECISION) AS total_spent,
#                 COUNT(o.id) AS num_order
#             FROM client c
#             JOIN orders o ON c.id = o.client_id
#             GROUP BY c.id, c.name, c.email, c.phone, c.address
#         """
#     df = spark.read \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres_container:5432/postgres") \
#         .option("dbtable", f"({query}) AS subquery") \
#         .option("user", "admin") \
#         .option("password", "123456") \
#         .option("driver", "org.postgresql.Driver") \
#         .load()
#
#     df = transform_customer(df)
#     print(df.head(1257))
#     # spark.sql("""
#     #    SELECT c.id
#     #    FROM client c
#     #    WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.client_id = c.id)
#     # """).show()
#     df_pd = df.toPandas()
#     return df_pd
#
# def transform_customer(df):
#     df = df.withColumn("email", trim(col("email")))
#     df = df.withColumn("phone", trim(col("phone").cast("string")))
#     df = df.withColumn("address", trim(col("address").cast("string")))
#
#     df = df.withColumn("email", when(col("email") == "", "N/A").otherwise(col("email")))
#     df = df.withColumn("phone", when(col("phone") == "", "N/A").otherwise(col("phone")))
#     df = df.withColumn("address", when(col("address") == "", "N/A").otherwise(col("address")))
#
#     df = df.fillna({"num_order": 0, "total_spent": 0.0})
#     df = df.withColumn("total_spent", when(col("total_spent") < 0, 0.0).otherwise(col("total_spent")))
#     df = df.withColumn("num_order", when(col("num_order") < 0, 0).cast(IntegerType()))
#     return df

def extract_customer():
    pg_conn = get_pg_conn()
    df = pd.read_sql("""
        SELECT
            c.id AS client_id,
            c.name as name,
            c.email as email,
            c.phone as phone,
            c.address as address,
            COALESCE(SUM(o.total_amount), 0) AS total_spent,
            COUNT(o.id) AS num_order
        FROM client c
        LEFT JOIN orders o ON c.id = o.client_id
        GROUP BY c.id, c.name, c.email, c.phone, c.address
    """, pg_conn)
    df["name"] = df["name"].astype(str)
    df["name"] = df["name"].apply(lambda x: str(x) if pd.notnull(x) else "")
    pg_conn.close()
    return df[["client_id", "name", "email", "phone", "address", "total_spent", "num_order"]]

def transform_customer(df):
    # # Bỏ khoảng trắng dư thừa và chuẩn hóa tên khách hàng
    # df["name"] = df["name"].str.strip().str.title()  # viết hoa chữ cái đầu

    # Bỏ khoảng trắng dư thừa trong email, phone, address
    df["email"] = df["email"].str.strip()
    df["phone"] = df["phone"].astype(str).str.strip()
    df["address"] = df["address"].astype(str).str.strip()

    # # Loại bỏ ký tự không hợp lệ trong số điện thoại (chỉ giữ số, +, -, dấu cách)
    # df["phone"] = df["phone"].str.replace(r"[^\d+\-\s]", "", regex=True)

    # Thay thế các giá trị null hoặc trống thành "N/A" cho email, phone, address nếu cần
    df["email"] = df["email"].replace("", "N/A").fillna("N/A")
    df["phone"] = df["phone"].replace("", "N/A").fillna("N/A")
    df["address"] = df["address"].replace("", "N/A").fillna("N/A")

    # Kiểm tra nếu total_spent < 0 thì gán về 0
    df["total_spent"] = df["total_spent"].apply(lambda x: max(x, 0))

    # Đảm bảo num_order là số nguyên không âm
    df["num_order"] = df["num_order"].apply(lambda x: max(int(x), 0))

    return df

def send_customer_to_kafka(df, batch_size = 100):
    print(f"Tổng số bản ghi cần gửi: {len(df)}")
    batch_size = int(batch_size)
    ensure_topic_exists(TOPIC_NAME)
    producer = get_producer()

    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i + batch_size]
        print(f"Đang gửi batch {i // batch_size + 1} với {len(batch_df)} bản ghi")

        for _, row in batch_df.iterrows():
            customer_msg = CustomerMessage(
                client_id=row["client_id"],
                name=row["name"],
                email=row["email"],
                phone=row["phone"],
                address=row["address"],
                total_spent=float(row["total_spent"]),
                num_order=row["num_order"]
            )
            producer.produce(
                topic=TOPIC_NAME,
                key=str(row["client_id"]),
                value=customer_msg,
                on_delivery=delivery_report
            )
        producer.flush()

def load_customer():
    protobuf_deserializer = ProtobufDeserializer(
        message_type=CustomerMessage,
        conf={"schema.registry.url": SCHEMA_REGISTRY_URL}
    )

    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'dim_customer_group',
        'auto.offset.reset': 'earliest',
    }

    consumer = DeserializingConsumer({
        **consumer_conf,
        'value.deserializer': lambda data, ctx: protobuf_deserializer(data, ctx),
    })

    consumer.subscribe(['dim_customer'])
    sql_conn = get_sqlserver_conn()
    cursor = sql_conn.cursor()

    inserted_count = 0
    skipped_count = 0
    empty_poll_count = 0

    while empty_poll_count < 10:
        msg = consumer.poll(1.0)
        if msg is None:
            print("Không có message nào mới.")
            empty_poll_count += 1
            continue

        customer = msg.value()

        cursor.execute("""
            SELECT 1 FROM dim_customer WHERE client_id = ?
        """, customer.client_id)

        if cursor.fetchone() is None:
            cursor.execute("""
                INSERT INTO dim_customer (client_id, name, email, phone, address, total_spent, num_order)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, customer.client_id, str(customer.name), customer.email, customer.phone, customer.address, customer.total_spent, customer.num_order)
            inserted_count += 1
        else:
            skipped_count +=1

    sql_conn.commit()
    print(f"Đã thêm dim_customer {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")
    cursor.close()
    sql_conn.close()
    consumer.close()