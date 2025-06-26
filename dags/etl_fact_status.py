import time

import pandas as pd
import logging

from pyspark.sql.functions import count, lit, col, udf, when
from pyspark.sql.types import IntegerType

from spark.build_spark import get_spark, read_spark
from monitoring.build_logging import setup_logging
from kafka.build_kafka import get_producer, ensure_topic_exists, delivery_report, get_consumer
from google.protobuf.message_factory import GetMessageClass
from dags.database.db_connections import get_pg_conn, get_sqlserver_conn
from proto.generated import status_pb2

TOPIC_NAME = 'fact_status'
GROUP_ID = 'fact_status_group'
BATCH_SIZE = 5000

# Nếu bạn đã có DESCRIPTOR từ file .proto generated
descriptor = status_pb2.DESCRIPTOR.message_types_by_name['Status']

# Tạo dynamic message class từ descriptor
StatusMessage = GetMessageClass(descriptor)

# logger
setup_logging()
logger = logging.getLogger(__name__)

def get_status_dimension_mappings():
    mappings = {}
    sql_conn = get_sqlserver_conn()
    try:
        date_df = pd.read_sql("SELECT date, date_key FROM dim_date", sql_conn)
        logger.info(date_df['date'].dtype)
        date_df['date'] = date_df['date'].astype(str)
        mappings['date'] = dict(zip(date_df['date'], date_df['date_key']))

        order_df = pd.read_sql("SELECT order_item_id, order_key FROM dim_order", sql_conn)
        mappings['order'] = dict(zip(order_df['order_item_id'], order_df['order_key']))

        return mappings
    except Exception as e:
        logger.error(f"Lỗi khi lấy dimension mappings: {e}")
        raise
    finally:
        sql_conn.close()

def extract_transform_fact_status(spark, offset, batch_size):
    logger.info("extract fact_status")
    query = f"""
        SELECT 
            oi.id as order_item_id,
            DATE_TRUNC('day', oi.created_date)::DATE AS order_date,
            oi.status,
            COUNT(*) AS total_orders
        FROM order_items oi
        JOIN orders o on oi.order_id = o.id
        WHERE oi.status IN ('DELIVERED', 'CANCELLED') AND o.status = 'COMPLETED'
        GROUP BY oi.id, DATE_TRUNC('day', oi.created_date), oi.status
        OFFSET {offset} LIMIT {batch_size}
    """
    df = read_spark(spark, query)
    # Pivot để chuyển từ dòng thành cột theo trạng thái
    logger.info("pivot để chuyển từ dòng thành cột theo trạng thái")
    pivot_df = (
        df.groupBy("order_item_id", "order_date", "status")
        .agg(count(lit(1)).alias("total_orders"))
        .groupBy("order_item_id", "order_date")
        .pivot("status", ["DELIVERED", "CANCELLED"])
        .sum("total_orders")
        .fillna(0)
    )

    # Đảm bảo có 2 cột chính cần dùng dù không có dữ liệu
    if 'DELIVERED' not in pivot_df.columns:
        pivot_df = pivot_df.withColumn("DELIVERED", lit(0))
    if 'CANCELLED' not in pivot_df.columns:
        pivot_df = pivot_df.withColumn("CANCELLED", lit(0))

    pivot_df = pivot_df.withColumnRenamed("DELIVERED", "delivered_count") \
        .withColumnRenamed("CANCELLED", "cancelled_count")

    # Tính phần trăm đơn giao và hủy (sử dụng tên cột mới)
    pivot_df = pivot_df.withColumn("total", col("delivered_count") + col("cancelled_count"))
    pivot_df = pivot_df.withColumn("delivery_percentage",
                                 when(col("total") == 0, 0.0)
                                 .otherwise((100 * col("delivered_count") / col("total")).cast("double")))
    pivot_df = pivot_df.withColumn("cancel_percentage",
                                 when(col("total") == 0, 0.0)
                                 .otherwise((100 * col("cancelled_count") / col("total")).cast("double")))
    logger.info("transform fact_status")
    logger.info(f"Số lượng bản ghi query được: {df.count()}")
    df.printSchema()
    pivot_df = transform_fact_status(spark, pivot_df)
    logger.info("Extract và transform fact_status thành công")
    return pivot_df

def transform_fact_status(spark, df):
    df = df.withColumn("order_date", col("order_date").cast("string"))
    logger.info("Mapping dimension keys cho fact_status")
    # Lấy mappings
    mappings = get_status_dimension_mappings()
    date_mapping = mappings['date']
    order_mapping = mappings['order']

    # Broadcast mappings
    sc = spark.sparkContext
    broadcast_date = sc.broadcast(date_mapping)
    broadcast_order = sc.broadcast(order_mapping)

    # UDF dùng broadcast
    date_udf = udf(lambda x: broadcast_date.value.get(str(x)), IntegerType())
    order_udf = udf(lambda x: broadcast_order.value.get(x), IntegerType())

    # Thêm các cột khóa ngoại
    df = df.withColumn("date_key", date_udf(col("order_date"))) \
           .withColumn("order_key", order_udf(col("order_item_id")))

    logger.info("Lọc bản ghi thiếu dimension keys")
    df = df.na.drop(subset=['date_key', 'order_key'])

    return df

def producer_status_kafka ():
    spark = get_spark()
    producer = get_producer(StatusMessage)
    ensure_topic_exists(TOPIC_NAME)

    batch_number = 1
    offset = 0
    total_records = 0

    try:
        while True:
            time.sleep(0.1)
            df = extract_transform_fact_status(spark, offset, BATCH_SIZE)
            if df.isEmpty():
                logger.info("Đã hết dữ liệu để push lên Kafka.")
                break

            logger.info(f"Tổng số bản ghi cần gửi: {df.count()}")
            logger.info(f"Batch {batch_number}: {df.count()} bản ghi. Đang gửi lên Kafka...")

            for row in df.toLocalIterator():
                status_msg = StatusMessage(
                    order_key=int(row["order_key"]),
                    date_key=int(row["date_key"]),
                    delivered_count=int(row["delivered_count"]),
                    cancelled_count=int(row["cancelled_count"]),
                    delivery_percentage=float(row["delivery_percentage"]),
                    cancel_percentage=float(row["cancel_percentage"])
                )
                producer.produce(
                    topic=TOPIC_NAME,
                    key=f"{row['order_key']}-{row['date_key']}",
                    value=status_msg,
                    # on_delivery=delivery_report
                )
            producer.flush()
            total_records += df.count()
            offset += BATCH_SIZE
            batch_number += 1
        logger.info(f"Đã đẩy {total_records} bản ghi lên Kafka.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình gửi Kafka: {e}")
    finally:
        producer.flush()
        spark.stop()

def consumer_fact_status_kafka():
    consumer = get_consumer(StatusMessage, GROUP_ID)
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

            status = msg.value()

            cursor.execute("""
                SELECT 1 FROM fact_status 
                WHERE order_key = ? AND date_key = ?
            """, (status.order_key, status.date_key))

            if cursor.fetchone() is None:
                cursor.execute("""
                    INSERT INTO fact_status (
                        order_key, date_key, 
                        delivered_count, cancelled_count, 
                        delivery_percentage, cancel_percentage
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    status.order_key, status.date_key,
                    status.delivered_count, status.cancelled_count,
                    status.delivery_percentage, status.cancel_percentage
                ))
                inserted_count += 1
            else:
                skipped_count += 1

        logger.info(f"Đã thêm fact_status {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")

    except Exception as e:
        sql_conn.rollback()
        logger.error(f"Lỗi trong quá trình consume hoặc insert: {e}")
        raise
    finally:
        sql_conn.commit()
        cursor.close()
        sql_conn.close()
        consumer.close()
