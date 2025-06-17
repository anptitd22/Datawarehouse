# etl_fact_sales.py
import pandas as pd
from db_connections import get_pg_conn, get_sqlserver_conn

def get_status_dimension_mappings():
    mappings = {}
    sql_conn = get_sqlserver_conn()
    try:
        date_df = pd.read_sql("SELECT date, date_key FROM dim_date", sql_conn)
        mappings['date'] = dict(zip(date_df['date'], date_df['date_key']))

        order_df = pd.read_sql("SELECT order_id, order_key FROM dim_order", sql_conn)
        mappings['order'] = dict(zip(order_df['order_id'], order_df['order_key']))

        return mappings
    except Exception as e:
        print("Lỗi rồi")
        raise
    finally:
        sql_conn.close()

def extract_fact_status():
    pg_conn = get_pg_conn()
    df = pd.read_sql("""
        SELECT 
            o.id as order_id,
            DATE_TRUNC('day', oi.created_date)::DATE AS order_date,
            oi.status,
            COUNT(*) AS total_orders
        FROM order_items oi
        JOIN orders o on oi.order_id = o.id
        WHERE oi.status IN ('DELIVERED', 'CANCELLED')
        GROUP BY o.id, DATE_TRUNC('day', oi.created_date), oi.status
    """, pg_conn)
    pg_conn.close()

    # Pivot để chuyển từ dòng thành cột theo trạng thái
    pivot_df = df.pivot_table(
        index=['order_id', 'order_date'],
        columns='status',
        values='total_orders',
        fill_value=0
    ).reset_index()

    pivot_df.columns.name = None
    pivot_df = pivot_df.rename(columns={
        'DELIVERED': 'delivered_count',
        'CANCELLED': 'cancelled_count'
    })

    # Tính phần trăm đơn giao và hủy
    pivot_df['total'] = pivot_df['delivered_count'] + pivot_df['cancelled_count']
    pivot_df['delivery_percentage'] = round(100 * pivot_df['delivered_count'] / pivot_df['total'], 2)
    pivot_df['cancel_percentage'] = round(100 * pivot_df['cancelled_count'] / pivot_df['total'], 2)

    return pivot_df


def transform_fact_status(df, mappings):
    try:
        df['date_key'] = df['order_date'].map(mappings['date'])
        df['order_key'] = df['order_id'].map(mappings['order'])

        initial = len(df)
        df = df.dropna(subset=['date_key', 'order_key'])
        if len(df) < initial:
            print(f"Đã loại {initial - len(df)} dòng do thiếu khóa")

        final_df = df[[
            'order_key', 'date_key',
            'delivered_count', 'cancelled_count',
            'delivery_percentage', 'cancel_percentage'
        ]]
        return final_df
    except Exception as e:
        print("Lỗi trong transform_fact_status:", str(e))
        raise


def load_fact_status(df):
    sql_conn = get_sqlserver_conn()
    cursor = sql_conn.cursor()
    inserted_count = 0
    skipped_count = 0
    try:
        for _, row in df.iterrows():
            cursor.execute("""
                SELECT 1 FROM fact_status 
                WHERE order_key = ? AND date_key = ?
            """, (row.order_key, row.date_key))

            if cursor.fetchone() is None:
                cursor.execute("""
                    INSERT INTO fact_status (
                        order_key, date_key, 
                        delivered_count, cancelled_count, 
                        delivery_percentage, cancel_percentage
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    row.order_key, row.date_key,
                    row.delivered_count, row.cancelled_count,
                    row.delivery_percentage, row.cancel_percentage
                ))
                inserted_count += 1
            else:
                skipped_count += 1

            sql_conn.commit()

        print(f"Đã thêm fact_sales {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")

    except Exception as e:
        sql_conn.rollback()
        print(f"Lỗi khi tải dữ liệu: {str(e)}")
        raise
    finally:
        cursor.close()
        sql_conn.close()
