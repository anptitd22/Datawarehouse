import pandas as pd
from db_connections import get_pg_conn, get_sqlserver_conn

def extract_product():
    pg_conn = get_pg_conn()
    df = pd.read_sql("""
        SELECT 
            p.id AS product_id, p.name, p.brand, c.name AS category,
            p.import_price, p.selling_price, p.promotion_price, p.rating, p.sold, p.stock
        FROM product p
        JOIN category c ON p.category_id = c.id
        WHERE p.is_deleted = FALSE
    """, pg_conn)
    pg_conn.close()
    return df

def transform_product(df):
    # Bỏ các khoảng trắng dư thừa trong tên và brand
    df["name"] = df["name"].str.strip()
    df["brand"] = df["brand"].str.strip()
    df["category"] = df["category"].str.strip()

    # Thay thế các giá trị NULL hoặc NaN trong cột numeric bằng 0
    numeric_cols = ["import_price", "selling_price", "promotion_price", "rating", "sold", "stock"]
    df[numeric_cols] = df[numeric_cols].fillna(0)

    return df

def load_product(df):
    sql_conn = get_sqlserver_conn()
    cursor = sql_conn.cursor()

    inserted_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        # Kiểm tra trùng lặp
        cursor.execute("""
                SELECT 1 FROM dim_product WHERE product_id = ?
            """, row.product_id)
        if cursor.fetchone() is None:
            # Không tìm thấy bản ghi trùng, thực hiện insert
            cursor.execute("""
                INSERT INTO dim_product (product_id, name, brand, category, import_price, selling_price, promotion_price, rating,sold, stock)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row.product_id, str(row["name"]), row.brand, row.category, row.import_price, row.selling_price, row.promotion_price, row.rating,row.sold, row.stock)
            inserted_count += 1
        else:
            skipped_count += 1

    sql_conn.commit()

    print(f"Đã thêm dim_product {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")
    cursor.close()
    sql_conn.close()
