# etl_fact_sales.py
import pandas as pd
from db_connections import get_pg_conn, get_sqlserver_conn


def get_dimension_mappings():
    """Lấy ánh xạ khóa dimension từ SQL Server"""
    mappings = {}
    sql_conn = get_sqlserver_conn()

    try:
        # Dim Date mapping
        date_df = pd.read_sql("SELECT date, date_key FROM dim_date", sql_conn)
        mappings['date'] = dict(zip(date_df['date'], date_df['date_key']))

        # Dim Customer mapping
        customer_df = pd.read_sql("SELECT client_id, customer_key FROM dim_customer", sql_conn)
        mappings['customer'] = dict(zip(customer_df['client_id'], customer_df['customer_key']))

        # Dim Product mapping
        product_df = pd.read_sql("SELECT product_id, product_key FROM dim_product", sql_conn)
        mappings['product'] = dict(zip(product_df['product_id'], product_df['product_key']))

        # Dim Order mapping
        order_df = pd.read_sql("SELECT order_id, order_key FROM dim_order", sql_conn)
        mappings['order'] = dict(zip(order_df['order_id'], order_df['order_key']))

        return mappings

    except Exception as e:
        print("Lỗi rồi")
        raise
    finally:
        sql_conn.close()

def extract_fact_sales():
    """Trích xuất dữ liệu từ PostgreSQL"""
    query = """
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
    WHERE o.status = 'COMPLETED'
    """

    pg_conn = get_pg_conn()
    try:
        df = pd.read_sql(query, pg_conn)
        return df
    except Exception as e:
        raise
    finally:
        pg_conn.close()

def transform_fact_sales(df, dim_mappings):
    """Chuyển đổi dữ liệu fact_sales"""
    try:
        # Map dimension keys
        df['date_key'] = df['order_date'].map(dim_mappings['date'])
        df['customer_key'] = df['client_id'].map(dim_mappings['customer'])
        df['product_key'] = df['product_id'].map(dim_mappings['product'])
        df['order_key'] = df['order_id'].map(dim_mappings['order'])

        # Filter out records with missing dimension keys
        initial_count = len(df)
        df = df.dropna(subset=['date_key', 'customer_key', 'product_key', 'order_key'])

        if len(df) < initial_count:
            print("cẩn thận")

        # Select only needed columns
        final_df = df[[
            'order_item_id', 'order_id',
            'customer_key', 'product_key', 'date_key', 'order_key',
            'quantity', 'unit_price', 'total_price', 'cost', 'revenue', 'profit'
        ]]

        return final_df

    except Exception as e:
        print("Lỗi rồi")
        raise

def load_fact_sales(df):
    """Tải dữ liệu vào fact_sales với kiểm tra trùng lặp"""
    sql_conn = get_sqlserver_conn()
    cursor = sql_conn.cursor()

    # Đếm số bản ghi đã xử lý và bỏ qua
    inserted_count = 0
    skipped_count = 0

    try:
        for _, row in df.iterrows():
            # Kiểm tra trùng lặp dựa trên các khóa dimension
            cursor.execute("""
                SELECT 1 FROM fact_sales 
                WHERE order_key = ? 
                  AND customer_key = ?
                  AND product_key = ?
                  AND date_key = ?
            """, (row.order_key, row.customer_key, row.product_key, row.date_key))

            if cursor.fetchone() is None:
                # Không tìm thấy bản ghi trùng, thực hiện insert
                cursor.execute("""
                    INSERT INTO fact_sales (
                        order_key, customer_key, product_key, date_key,
                        quantity, unit_price, total_price, cost, revenue, profit
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.order_key, row.customer_key, row.product_key, row.date_key,
                    row.quantity, row.unit_price, row.total_price,
                    row.cost, row.revenue, row.profit
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
