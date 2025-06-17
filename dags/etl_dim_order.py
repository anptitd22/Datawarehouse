import pandas as pd
from db_connections import get_pg_conn, get_sqlserver_conn

def extract_order():
    pg_conn = get_pg_conn()
    df = pd.read_sql("""
        SELECT
        o.id as order_id,
        oi.id as order_item_id,
        oi.status as status,
        o.method_payment as payment_method
        From order_items oi
        join orders o on oi.order_id = o.id
    """, pg_conn)
    pg_conn.close()
    return df[["order_id", "order_item_id", "status", "payment_method"]]

def transform_order(df):
    # Bỏ các khoảng trắng dư thừa trong tên
    df["status"] = df["status"].str.strip()
    df["payment_method"] = df["payment_method"].str.strip()
    return df

def load_order(df):
    sql_conn = get_sqlserver_conn()
    cursor = sql_conn.cursor()

    inserted_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        # Kiểm tra trùng lặp
        cursor.execute("""
            SELECT 1 FROM dim_order WHERE order_id = ?
        """, row.order_id)

        if cursor.fetchone() is None:
            # Không tìm thấy bản ghi trùng, thực hiện insert
            cursor.execute("""
                INSERT INTO dim_order (order_id, order_item_id, status, payment_method)
                VALUES (?, ?, ?, ?)
            """, row.order_id, row.order_item_id, row.status, row.payment_method)
            inserted_count += 1
        else:
            skipped_count +=1
    sql_conn.commit()
    print(f"Đã thêm dim_order {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")
    cursor.close()
    sql_conn.close()