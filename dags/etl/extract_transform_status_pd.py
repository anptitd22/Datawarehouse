import pandas as pd
import logging

logger = logging.getLogger(__name__)

def get_status_dimension_mappings():
    mappings = {}
    sql_conn = get_sqlserver_conn()
    try:
        date_df = pd.read_sql("SELECT date, date_key FROM dim_date", sql_conn)
        mappings['date'] = dict(zip(date_df['date'].astype(str), date_df['date_key']))

        order_df = pd.read_sql("SELECT order_item_id, order_key FROM dim_order", sql_conn)
        mappings['order'] = dict(zip(order_df['order_item_id'], order_df['order_key']))

        return mappings
    except Exception as e:
        logger.error(f"Lỗi khi lấy dimension mappings: {e}")
        raise
    finally:
        sql_conn.close()


def extract_transform_fact_status(offset, batch_size, conn):
    logger.info("Extract fact_status")
    query = f"""
        SELECT 
            oi.id AS order_item_id,
            DATE_TRUNC('day', oi.created_date)::DATE AS order_date,
            oi.status
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.id
        WHERE oi.status IN ('DELIVERED', 'CANCELLED') AND o.status = 'COMPLETED'
        OFFSET {offset} LIMIT {batch_size}
    """
    df = pd.read_sql_query(query, conn)

    logger.info("Pivot fact_status")
    # Pivot để chuyển từ dòng thành cột theo trạng thái
    pivot_df = df.pivot_table(
        index=['order_item_id', 'order_date'],
        columns='status',
        aggfunc='size',
        fill_value=0
    ).reset_index()

    # Đảm bảo cột đầy đủ
    if 'DELIVERED' not in pivot_df.columns:
        pivot_df['DELIVERED'] = 0
    if 'CANCELLED' not in pivot_df.columns:
        pivot_df['CANCELLED'] = 0

    pivot_df = pivot_df.rename(columns={
        'DELIVERED': 'delivered_count',
        'CANCELLED': 'cancelled_count'
    })

    # Tính toán phần trăm
    pivot_df['total'] = pivot_df['delivered_count'] + pivot_df['cancelled_count']
    pivot_df['delivery_percentage'] = (pivot_df['delivered_count'] / pivot_df['total'] * 100).round(2)
    pivot_df['cancel_percentage'] = (pivot_df['cancelled_count'] / pivot_df['total'] * 100).round(2)

    logger.info("Transform fact_status")
    pivot_df = transform_fact_status(pivot_df)

    logger.info("Extract và transform fact_status thành công")
    return pivot_df


def transform_fact_status(df: pd.DataFrame) -> pd.DataFrame:
    mappings = get_status_dimension_mappings()

    logger.info("Mapping dimension keys cho fact_status")
    df['date_key'] = df['order_date'].astype(str).map(mappings['date'])
    df['order_key'] = df['order_item_id'].map(mappings['order'])

    logger.info("Lọc bản ghi thiếu dimension keys")
    df.dropna(subset=['date_key', 'order_key'], inplace=True)

    # Đảm bảo kiểu dữ liệu keys là int
    df = df.astype({'date_key': 'int', 'order_key': 'int'})

    return df
