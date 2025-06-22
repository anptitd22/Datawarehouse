import time

import pandas as pd
from pyspark.sql.functions import col, trim
from pyspark.sql.types import DoubleType, IntegerType

from spark.build_spark import get_spark, read_spark
from monitoring.build_logging import setup_logging
import logging
from kafka.build_kafka import get_producer, ensure_topic_exists, delivery_report, get_consumer
from google.protobuf.message_factory import GetMessageClass
from dags.database.db_connections import get_pg_conn, get_sqlserver_conn
from proto.generated import product_pb2

TOPIC_NAME = 'dim_product'
GROUP_ID = 'dim_product_group'
BATCH_SIZE = 2000

# Nếu bạn đã có DESCRIPTOR từ file .proto generated
descriptor = product_pb2.DESCRIPTOR.message_types_by_name['Product']

# Tạo dynamic message class từ descriptor
ProductMessage = GetMessageClass(descriptor)

# logger
setup_logging()
logger = logging.getLogger(__name__)

def extract_transform_product(spark, offset, batch_size):
    logger.info("extract dim_product")
    query = f"""
        SELECT 
            p.id AS product_id, p.name, p.brand, c.name AS category,
            p.import_price, p.selling_price, p.promotion_price, p.rating, p.sold, p.stock
        FROM product p
        JOIN category c ON p.category_id = c.id
        WHERE p.is_deleted = FALSE
        OFFSET {offset} LIMIT {batch_size}
    """
    df = read_spark(spark, query)
    logger.info("transform dim_product")
    df = transform_product(df)
    logger.info("Extract và transform dim_product thành công")
    return df

def transform_product(df):
    # Trim các cột text
    text_cols = ["name", "brand", "category"]
    for col_name in text_cols:
        df = df.withColumn(col_name, trim(col(col_name).cast("string")))

    # Điền NULL/NaN trong các cột numeric bằng 0
    numeric_fill = {
        "import_price": 0.0,
        "selling_price": 0.0,
        "promotion_price": 0.0,
        "rating": 0.0,
        "sold": 0,
        "stock": 0,
    }
    df = df.fillna(numeric_fill)

    # Đảm bảo kiểu dữ liệu đúng cho numeric
    df = df.withColumn("import_price", col("import_price").cast(DoubleType()))
    df = df.withColumn("selling_price", col("selling_price").cast(DoubleType()))
    df = df.withColumn("promotion_price", col("promotion_price").cast(DoubleType()))
    df = df.withColumn("rating", col("rating").cast(DoubleType()))
    df = df.withColumn("sold", col("sold").cast(IntegerType()))
    df = df.withColumn("stock", col("stock").cast(IntegerType()))

    return df

def producer_product_kafka ():
    spark = get_spark()
    producer = get_producer(ProductMessage)
    ensure_topic_exists(TOPIC_NAME)

    batch_number = 1
    offset = 0
    total_records = 0

    try:
        while True:
            time.sleep(0.4)
            df = extract_transform_product(spark, offset, BATCH_SIZE)
            if df.isEmpty():
                logger.info("Đã hết dữ liệu để push lên Kafka.")
                break

            logger.info(f"Tổng số bản ghi cần gửi: {df.count()}")
            logger.info(f"Batch {batch_number}: {df.count()} bản ghi. Đang gửi lên Kafka...")

            for row in df.toLocalIterator():
                product_msg = ProductMessage(
                    product_id=str(row["product_id"]),
                    name=row["name"],
                    brand=row["brand"],
                    category=row["category"],
                    import_price = row["import_price"],
                    selling_price = row["selling_price"],
                    promotion_price = row["promotion_price"],
                    sold = row["sold"],
                    stock = row["stock"],
                    rating = row["rating"]
                )
                producer.produce(
                    topic=TOPIC_NAME,
                    key=str(row["product_id"]),
                    value=product_msg,
                    on_delivery=delivery_report
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

def consumer_product_kafka():
    consumer = get_consumer(ProductMessage, GROUP_ID)
    consumer.subscribe([TOPIC_NAME])

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

            product = msg.value()
            # Kiểm tra trùng lặp
            cursor.execute("""
                    SELECT 1 FROM dim_product WHERE product_id = ?
                """, product.product_id)
            if cursor.fetchone() is None:
                # Không tìm thấy bản ghi trùng, thực hiện insert
                cursor.execute("""
                    INSERT INTO dim_product (product_id, name, brand, category, import_price, selling_price, promotion_price, rating,sold, stock)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, product.product_id, product.name, product.brand, product.category, product.import_price, product.selling_price, product.promotion_price, product.rating,product.sold, product.stock)
                inserted_count += 1
            else:
                skipped_count += 1
        except Exception as e:
            logger.error(f"Lỗi trong quá trình consume hoặc insert: {e}")

    sql_conn.commit()

    print(f"Đã thêm dim_product {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")
    cursor.close()
    sql_conn.close()
    consumer.close()
