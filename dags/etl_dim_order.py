import time

import pandas as pd
from pyspark.sql.functions import trim, col

from spark.build_spark import get_spark, read_spark
from monitoring.build_logging import setup_logging
import logging
from kafka.build_kafka import get_producer, ensure_topic_exists, delivery_report, get_consumer
from google.protobuf.message_factory import GetMessageClass
from dags.database.db_connections import get_pg_conn, get_sqlserver_conn
from proto.generated import order_pb2

TOPIC_NAME = 'dim_order'
GROUP_ID = 'dim_order_group'
BATCH_SIZE = 5000

# Nếu bạn đã có DESCRIPTOR từ file .proto generated
descriptor = order_pb2.DESCRIPTOR.message_types_by_name['Order']

# Tạo dynamic message class từ descriptor
OrderMessage = GetMessageClass(descriptor)

# logger
setup_logging()
logger = logging.getLogger(__name__)

def extract_transform_order(spark, offset, batch_size):
    logger.info("extract_dim_order")
    query = f"""
        SELECT
        o.id as order_id,
        oi.id as order_item_id,
        oi.status as status,
        o.method_payment as payment_method
        From order_items oi
        join orders o on oi.order_id = o.id
        OFFSET {offset} LIMIT {batch_size}
    """
    df = read_spark(spark, query)
    logger.info("transform dim_order")
    df = transform_order(df)
    logger.info("Extract và transform dim_order thành công")
    return df

def transform_order(df):
    # Bỏ các khoảng trắng dư thừa trong tên
    df = df.withColumn("status", trim(col("status")))
    df = df.withColumn("payment_method", trim(col("payment_method")))
    return df

def producer_order_kafka ():
    spark = get_spark()
    producer = get_producer(OrderMessage)
    ensure_topic_exists(TOPIC_NAME)

    batch_number = 1
    offset = 0
    total_records = 0

    try:
        while True:
            time.sleep(0.1)
            df = extract_transform_order(spark, offset, BATCH_SIZE)

            if df.isEmpty():
                logger.info("Đã hết dữ liệu để push lên Kafka.")
                break

            logger.info(f"Tổng số bản ghi cần gửi: {df.count()}")
            logger.info(f"Batch {batch_number}: {df.count()} bản ghi. Đang gửi lên Kafka...")

            for row in df.toLocalIterator():
                order_msg = OrderMessage(
                    order_id=row["order_id"],
                    order_item_id=str(row["order_item_id"]),
                    status=row["status"],
                    payment_method=row["payment_method"]
                )
                producer.produce(
                    topic=TOPIC_NAME,
                    key=str(row["order_item_id"]),
                    value=order_msg,
                    # on_delivery=delivery_report
                )
            producer.flush()
            total_records += df.count()
            offset += BATCH_SIZE
            batch_number += 1

        logger.info(f"Đã đẩy {total_records} bản ghi lên Kafka.")

    except Exception as e:
        logger.error(f"Lỗi trong quá trình gửi Kafka: {e}")
        raise
    finally:
        producer.flush()
        spark.stop()

def consumer_order_kafka():
    consumer = get_consumer(OrderMessage, GROUP_ID)
    consumer.subscribe([TOPIC_NAME])

    sql_conn = get_sqlserver_conn()
    cursor = sql_conn.cursor()

    inserted_count = 0
    skipped_count = 0
    empty_poll_count = 0

    try:
        while empty_poll_count < 5:
            msg = consumer.poll(1.0)
            if msg is None:
                time.sleep(0.1)
                print("Không có message nào mới.")
                empty_poll_count += 1
                continue

            order = msg.value()

            # Kiểm tra trùng lặp
            cursor.execute("""
                SELECT 1 FROM dim_order WHERE order_item_id = ?
            """, order.order_item_id)

            if cursor.fetchone() is None:
                # Không tìm thấy bản ghi trùng, thực hiện insert
                cursor.execute("""
                    INSERT INTO dim_order (order_id, order_item_id, status, payment_method)
                    VALUES (?, ?, ?, ?)
                """, order.order_id, order.order_item_id, order.status, order.payment_method)
                inserted_count += 1
            else:
                skipped_count +=1

        logger.info(f"Đã thêm dim_order {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")

    except Exception as e:
        sql_conn.rollback()
        logger.error(f"Lỗi trong quá trình consume hoặc insert: {e}")
        raise
    finally:
        sql_conn.commit()
        cursor.close()
        sql_conn.close()
        consumer.close()