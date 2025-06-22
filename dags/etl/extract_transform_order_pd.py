import pandas as pd
import logging

logger = logging.getLogger(__name__)


def extract_transform_order_pd(offset, batch_size, conn):
    logger.info("Extract dim_order")

    query = f"""
        SELECT
            o.id AS order_id,
            oi.id AS order_item_id,
            oi.status AS status,
            o.method_payment AS payment_method
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.id
        OFFSET {offset} LIMIT {batch_size}
    """

    df = pd.read_sql_query(query, conn)

    logger.info("Transform dim_order")
    df = transform_order(df)

    logger.info("Extract và transform dim_order thành công")
    return df


def transform_order(df: pd.DataFrame) -> pd.DataFrame:
    df["status"] = df["status"].astype(str).str.strip()
    df["payment_method"] = df["payment_method"].astype(str).str.strip()
    return df
