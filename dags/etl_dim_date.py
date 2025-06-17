# etl_dim_date.py
import pandas as pd
from db_connections import get_pg_conn, get_sqlserver_conn


def extract_dates():
    pg_conn = get_pg_conn()
    df = pd.read_sql("""
        SELECT
        d::DATE AS date,
        EXTRACT(DAY FROM d)::INT as day,
        EXTRACT(WEEK FROM d)::INT as week,
        EXTRACT(MONTH FROM d)::INT as month,
        EXTRACT(QUARTER FROM d)::INT as quarter,
        EXTRACT(YEAR FROM d)::INT as year
        FROM generate_series('2022-01-01'::DATE, '2025-12-31'::DATE, '1 day') d;
    """, pg_conn)
    pg_conn.close()

    # df["day"] = df["date"].dt.day
    # df["week"] = df["date"].dt.isocalendar().week
    # df["month"] = df["date"].dt.month
    # df["quarter"] = df["date"].dt.quarter
    # df["year"] = df["date"].dt.year
    # df["date_id"] = df["date"].dt.strftime('%Y%m%d')  # bạn có thể dùng UUID nếu thích
    return df[["date", "day", "week", "month", "quarter", "year"]]

def transform_dates(df):
    return df

def load_date(df):
    sql_conn = get_sqlserver_conn()
    cursor = sql_conn.cursor()

    inserted_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        # Kiểm tra trùng lặp
        cursor.execute("""
            SELECT 1 FROM dim_date WHERE date = ?
        """, row.date)

        if cursor.fetchone() is None:
            cursor.execute("""
                INSERT INTO dim_date (date, day, week, month, quarter, year)
                VALUES (?, ?, ?, ?, ?, ?)
            """, row.date, row.day, row.week, row.month, row.quarter, row.year)
            inserted_count += 1
        else:
            skipped_count += 1

    sql_conn.commit()
    print(f"Đã thêm dim_date {inserted_count} bản ghi mới, bỏ qua {skipped_count} bản ghi trùng")
    cursor.close()
    sql_conn.close()
