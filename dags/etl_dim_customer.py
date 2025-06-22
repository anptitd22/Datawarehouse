import time

import pandas as pd
import logging
from monitoring.build_logging import setup_logging
from pyspark.sql.functions import trim, col, when
from pyspark.sql.types import IntegerType

from spark.build_spark import get_spark, read_spark
from kafka.build_kafka import get_producer, ensure_topic_exists, delivery_report, get_consumer
from google.protobuf.message_factory import GetMessageClass
from dags.database.db_connections import get_pg_conn, get_sqlserver_conn
from proto.generated import customer_pb2

TOPIC_NAME = 'dim_customer'
GROUP_ID = 'dim_customer_group'
BATCH_SIZE = 2000
# DESCRIPTOR
descriptor = customer_pb2.DESCRIPTOR.message_types_by_name['Customer']

# Tạo dynamic message class từ descriptor
CustomerMessage = GetMessageClass(descriptor)

#logger
setup_logging()
logger = logging.getLogger(__name__)

def extract_transform_customer(spark, offset, batch_size):
    logger.info("extract dim_customer")
    query = f"""
            SELECT
                c.id AS client_id,
                c.name as name,
                c.email as email,
                c.phone as phone,
                c.address as address,
                COALESCE(SUM(o.total_amount), 0) AS total_spent,
                COALESCE(COUNT(o.id), 0) AS num_order
            FROM client c
            JOIN orders o ON c.id = o.client_id
            GROUP BY c.id, c.name, c.email, c.phone, c.address
            OFFSET {offset} LIMIT {batch_size}
        """
    df = read_spark (spark, query)
    logger.info("transform_dim_customer")
    df = transform_customer(df)
    logger.info("extract_transform success")
    return df

def transform_customer(df):
    df = df.withColumn("email", trim(col("email")))
    df = df.withColumn("phone", trim(col("phone").cast("string")))
    df = df.withColumn("address", trim(col("address").cast("string")))

    df = df.withColumn("email", when(col("email") == "", "N/A").otherwise(col("email")))
    df = df.withColumn("phone", when(col("phone") == "", "N/A").otherwise(col("phone")))
    df = df.withColumn("address", when(col("address") == "", "N/A").otherwise(col("address")))

    df = df.fillna({"num_order": 0, "total_spent": 0.0})
    df = df.withColumn("total_spent", when(col("total_spent") < 0, 0.0).otherwise(col("total_spent")))
    df = df.withColumn("num_order", when(col("num_order") < 0, 0).cast(IntegerType()))
    return df

def producer_customer_kafka():
    spark = get_spark()
    producer = get_producer(CustomerMessage)
    ensure_topic_exists(TOPIC_NAME)
    offset = 0
    batch_number = 1
    total_records = 0
    try:
        while True:
            time.sleep(0.2)
            df = extract_transform_customer(spark, offset, BATCH_SIZE)

            if df.isEmpty():
                logger.info("Đã hết dữ liệu để push lên Kafka.")
                break

            logger.info(f"Tổng số bản ghi cần gửi: {df.count()}")
            logger.info(f"Batch {batch_number}: {df.count()} bản ghi. Đang gửi lên Kafka...")

            for row in df.toLocalIterator():
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

            total_records += df.count()
            offset += BATCH_SIZE
            batch_number += 1

        logger.info(f"Đã đẩy {total_records} dữ liệu lên Kafka.")

    except Exception as e:
        logger.error(f"Lỗi khi gửi Kafka batch {batch_number}: {e}")
    finally:
        producer.flush()
        spark.stop()

def consumer_customer_kafka():
    consumer = get_consumer(CustomerMessage, GROUP_ID)

    consumer.subscribe(['dim_customer'])
    sql_conn = get_sqlserver_conn()
    cursor = sql_conn.cursor()

    inserted_count = 0
    skipped_count = 0
    empty_poll_count = 0

    while empty_poll_count < 5:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                time.sleep(0.1)
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

        except Exception as e:
            logger.error(f"Lỗi trong quá trình consume hoặc insert: {e}")

    sql_conn.commit()
    print(f"Đã thêm dim_customer {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")
    cursor.close()
    sql_conn.close()
    consumer.close()