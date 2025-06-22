import time

import pandas as pd
import logging

from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

from spark.build_spark import get_spark, read_spark
from monitoring.build_logging import setup_logging
from kafka.build_kafka import get_producer, ensure_topic_exists, delivery_report, get_consumer
from google.protobuf.message_factory import GetMessageClass
from dags.database.db_connections import get_pg_conn, get_sqlserver_conn
from proto.generated import sales_pb2

TOPIC_NAME = 'fact_sales'
GROUP_ID = 'fact_sales_group'
BATCH_SIZE = 5000

# Nếu bạn đã có DESCRIPTOR từ file .proto generated
descriptor = sales_pb2.DESCRIPTOR.message_types_by_name['Sales']

# Tạo dynamic message class từ descriptor
SalesMessage = GetMessageClass(descriptor)

# logger
setup_logging()
logger = logging.getLogger(__name__)

def get_dimension_mappings():
    mappings = {}
    sql_conn = get_sqlserver_conn()
    try:
        date_df = pd.read_sql("SELECT date, date_key FROM dim_date", sql_conn)
        mappings['date'] = dict(zip(date_df['date'], date_df['date_key']))

        customer_df = pd.read_sql("SELECT client_id, customer_key FROM dim_customer", sql_conn)
        mappings['customer'] = dict(zip(customer_df['client_id'], customer_df['customer_key']))

        product_df = pd.read_sql("SELECT product_id, product_key FROM dim_product", sql_conn)
        mappings['product'] = dict(zip(product_df['product_id'], product_df['product_key']))

        order_df = pd.read_sql("SELECT order_item_id, order_key FROM dim_order", sql_conn)
        mappings['order'] = dict(zip(order_df['order_item_id'], order_df['order_key']))

        return mappings
    except Exception as e:
        logger.error(f"Lỗi khi lấy dimension mappings: {e}")
        raise
    finally:
        sql_conn.close()

def extract_transform_fact_sales(spark, offset, batch_size):
    logger.info("extract fact_sales")
    query = f"""
        SELECT 
            oi.id AS order_item_id,
            o.id AS order_id,
            o.client_id,
            oi.product_id,
            DATE_TRUNC('day', oi.created_date)::DATE AS order_date,
            oi.quantity,
            COALESCE(p.promotion_price, p.selling_price) AS unit_price,
            oi.total_price,
            p.import_price * oi.quantity AS cost,
            oi.total_price AS revenue,
            (oi.total_price - p.import_price * oi.quantity) AS profit
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.id
        JOIN product p ON oi.product_id = p.id
        WHERE o.status = 'COMPLETED' AND oi.status = 'DELIVERED'
        OFFSET {offset} LIMIT {batch_size}
    """
    df = read_spark(spark, query)
    logger.info("transform fact_sales")
    df = transform_fact_sales(spark, df)
    logger.info("Extract và transform fact_sales thành công")
    return df

def transform_fact_sales(spark, df):
    mappings = get_dimension_mappings()
    logger.info("Mapping dimension keys cho fact_sales")

    # Broadcast mappings
    sc = spark.sparkContext
    broadcast_date = sc.broadcast(mappings['date'])
    broadcast_customer = sc.broadcast(mappings['customer'])
    broadcast_product = sc.broadcast(mappings['product'])
    broadcast_order = sc.broadcast(mappings['order'])

    # UDF dùng broadcast
    date_udf = udf(lambda x: broadcast_date.value.get(x), IntegerType())
    customer_udf = udf(lambda x: broadcast_customer.value.get(x), IntegerType())
    product_udf = udf(lambda x: broadcast_product.value.get(x), IntegerType())
    order_udf = udf(lambda x: broadcast_order.value.get(x), IntegerType())

    df = df.withColumn("date_key", date_udf(col("order_date"))) \
           .withColumn("customer_key", customer_udf(col("client_id"))) \
           .withColumn("product_key", product_udf(col("product_id"))) \
           .withColumn("order_key", order_udf(col("order_item_id")))

    logger.info("Lọc bản ghi thiếu dimension keys")
    df = df.na.drop(subset=['date_key', 'customer_key', 'product_key', 'order_key'])
    return df

def producer_sales_kafka ():
    spark = get_spark()
    producer = get_producer(SalesMessage)
    ensure_topic_exists(TOPIC_NAME)

    batch_number = 1
    offset = 0
    total_records = 0

    try:
        while True:
            time.sleep(0.1)
            df = extract_transform_fact_sales(spark, offset, BATCH_SIZE)
            if df.isEmpty():
                logger.info("Đã hết dữ liệu để push lên Kafka.")
                break

            logger.info(f"Tổng số bản ghi cần gửi: {df.count()}")
            logger.info(f"Batch {batch_number}: {df.count()} bản ghi. Đang gửi lên Kafka...")

            for row in df.toLocalIterator():
                sales_msg = SalesMessage(
                    order_key=row["order_key"],
                    customer_key=row["customer_key"],
                    product_key=row["product_key"],
                    date_key=row["date_key"],
                    quantity = row["quantity"],
                    unit_price = row["unit_price"],
                    total_price = row["total_price"],
                    cost = row["cost"],
                    revenue = row["revenue"],
                    profit = row["profit"]
                )
                producer.produce(
                    topic=TOPIC_NAME,
                    key=f"{row['order_key']}-{row['customer_key']}-{row['product_key']}-{row['date_key']}",
                    value=sales_msg,
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

def consumer_fact_sales_kafka():
    consumer = get_consumer(SalesMessage, GROUP_ID)
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

            sales = msg.value()
            # Kiểm tra trùng lặp dựa trên các khóa dimension
            cursor.execute("""
                SELECT 1 FROM fact_sales 
                WHERE order_key = ? 
                  AND customer_key = ?
                  AND product_key = ?
                  AND date_key = ?
            """, (sales.order_key, sales.customer_key, sales.product_key, sales.date_key))

            if cursor.fetchone() is None:
                # Không tìm thấy bản ghi trùng, thực hiện insert
                cursor.execute("""
                    INSERT INTO fact_sales (
                        order_key, customer_key, product_key, date_key,
                        quantity, unit_price, total_price, cost, revenue, profit
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    sales.order_key, sales.customer_key, sales.product_key, sales.date_key,
                    sales.quantity, sales.unit_price, sales.total_price,
                    sales.cost, sales.revenue, sales.profit
                ))
                inserted_count += 1
            else:
                skipped_count += 1

        sql_conn.commit()
        print(f"Đã thêm fact_sales {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")

    except Exception as e:
        sql_conn.rollback()
        logger.error(f"Lỗi khi tải dữ liệu: {str(e)}")
        raise
    finally:
        cursor.close()
        sql_conn.close()
        consumer.close()
