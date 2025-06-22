import pandas as pd
import logging

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


def extract_transform_fact_sales(offset, batch_size, conn):
    logger.info("Extract fact_sales")
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
            (p.import_price * oi.quantity) AS cost,
            oi.total_price AS revenue,
            (oi.total_price - p.import_price * oi.quantity) AS profit
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.id
        JOIN product p ON oi.product_id = p.id
        WHERE o.status = 'COMPLETED' AND oi.status = 'DELIVERED'
        OFFSET {offset} LIMIT {batch_size}
    """
    df = pd.read_sql_query(query, conn)

    logger.info("Transform fact_sales")
    df = transform_fact_sales(df)

    logger.info("Extract và transform fact_sales thành công")
    return df


def transform_fact_sales(df: pd.DataFrame) -> pd.DataFrame:
    mappings = get_dimension_mappings()
    logger.info("Mapping dimension keys cho fact_sales")

    df['date_key'] = df['order_date'].map(mappings['date'])
    df['customer_key'] = df['client_id'].map(mappings['customer'])
    df['product_key'] = df['product_id'].map(mappings['product'])
    df['order_key'] = df['order_item_id'].map(mappings['order'])

    logger.info("Lọc bản ghi thiếu dimension keys")
    df.dropna(subset=['date_key', 'customer_key', 'product_key', 'order_key'], inplace=True)

    # Đảm bảo kiểu dữ liệu keys là int
    df = df.astype({'date_key': 'int', 'customer_key': 'int', 'product_key': 'int', 'order_key': 'int'})

    return df
