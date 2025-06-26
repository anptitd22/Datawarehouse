import logging
import time

from monitoring.build_logging import setup_logging
from pyspark.sql.functions import col
from kafka.build_kafka import get_producer, ensure_topic_exists, delivery_report, get_consumer
from google.protobuf.message_factory import GetMessageClass
from dags.database.db_connections import get_sqlserver_conn
from proto.generated import date_pb2
from spark.build_spark import get_spark, read_spark

TOPIC_NAME = 'dim_date'
GROUP_ID = 'dim_date_group'
BATCH_SIZE = 2000

# DESCRIPTOR
descriptor = date_pb2.DESCRIPTOR.message_types_by_name['Date']
DateMessage = GetMessageClass(descriptor)

# logger
setup_logging()
logger = logging.getLogger(__name__)


def extract_transform_dates(spark, offset, batch_size):
    logger.info("extract dim_date")
    query = f"""
        SELECT
            d::DATE AS date,
            EXTRACT(DAY FROM d)::INT as day,
            EXTRACT(WEEK FROM d)::INT as week,
            EXTRACT(MONTH FROM d)::INT as month,
            EXTRACT(QUARTER FROM d)::INT as quarter,
            EXTRACT(YEAR FROM d)::INT as year
        FROM generate_series('2022-01-01'::DATE, '2025-12-31'::DATE, '1 day') d
        OFFSET {offset} LIMIT {batch_size}
    """
    df = read_spark(spark, query)
    logger.info("transform dim_date")
    df = transform_dates(df)
    logger.info("Extract và transform dim_date thành công")
    return df

def transform_dates(df):
    # Nếu cần xử lý thêm dữ liệu thì thêm vào đây.
    return df


def producer_date_kafka():
    spark = get_spark()
    producer = get_producer(DateMessage)
    ensure_topic_exists(TOPIC_NAME)
    batch_number = 1
    offset = 0
    total_records = 0
    try:
        while True:
            time.sleep(0.3)
            df = extract_transform_dates(spark, offset, BATCH_SIZE)

            if df.isEmpty():
                logger.info("Đã hết dữ liệu để push lên Kafka.")
                break

            logger.info(f"Tổng số bản ghi cần gửi: {df.count()}")
            logger.info(f"Batch {batch_number}: {df.count()} bản ghi. Đang gửi lên Kafka...")

            for row in df.toLocalIterator():
                date_msg = DateMessage(
                    date=str(row["date"]),
                    day=row["day"],
                    week=row["week"],
                    month=row["month"],
                    quarter=row["quarter"],
                    year=row["year"]
                )
                producer.produce(
                    topic=TOPIC_NAME,
                    key=str(row["date"]),
                    value=date_msg,
                    on_delivery=delivery_report
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


def consumer_date_kafka():
    consumer = get_consumer(DateMessage, GROUP_ID)
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
                logger.info("Không có message nào mới.")
                empty_poll_count += 1
                continue

            date = msg.value()

            cursor.execute("SELECT 1 FROM dim_date WHERE date = ?", date.date)
            if cursor.fetchone() is None:
                cursor.execute("""
                    INSERT INTO dim_date (date, day, week, month, quarter, year)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, date.date, date.day, date.week, date.month, date.quarter, date.year)
                inserted_count += 1
            else:
                skipped_count += 1

        logger.info(f"Đã thêm dim_date {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")

    except Exception as e:
        sql_conn.rollback()
        logger.error(f"Lỗi trong quá trình consume hoặc insert: {e}")
        raise
    finally:
        sql_conn.commit()
        cursor.close()
        sql_conn.close()
        consumer.close()
