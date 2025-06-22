import pandas as pd
import logging

logger = logging.getLogger(__name__)


def extract_transform_dates_pd(offset, batch_size, conn):
    logger.info("Extract dim_date")

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

    df = pd.read_sql_query(query, conn)

    logger.info("Transform dim_date")
    df = transform_dates(df)

    logger.info("Extract và transform dim_date thành công")
    return df


def transform_dates(df: pd.DataFrame) -> pd.DataFrame:
    # Thêm xử lý dữ liệu ở đây nếu cần, hiện tại return nguyên gốc
    return df
