import pandas as pd
import logging

logger = logging.getLogger(__name__)


def extract_transform_product(offset, batch_size, conn):
    logger.info("Extract dim_product")

    query = f"""
        SELECT 
            p.id AS product_id, 
            p.name, 
            p.brand, 
            c.name AS category,
            p.import_price, 
            p.selling_price, 
            p.promotion_price, 
            p.rating, 
            p.sold, 
            p.stock
        FROM product p
        JOIN category c ON p.category_id = c.id
        WHERE p.is_deleted = FALSE
        OFFSET {offset} LIMIT {batch_size}
    """

    df = pd.read_sql_query(query, conn)

    logger.info("Transform dim_product")
    df = transform_product(df)

    logger.info("Extract và transform dim_product thành công")
    return df


def transform_product(df: pd.DataFrame) -> pd.DataFrame:
    # Trim text columns
    text_cols = ["name", "brand", "category"]
    for col_name in text_cols:
        df[col_name] = df[col_name].astype(str).str.strip()

    # Fill NULL/NaN for numeric columns
    numeric_fill = {
        "import_price": 0.0,
        "selling_price": 0.0,
        "promotion_price": 0.0,
        "rating": 0.0,
        "sold": 0,
        "stock": 0,
    }
    df.fillna(value=numeric_fill, inplace=True)

    # Đảm bảo kiểu dữ liệu đúng
    df["import_price"] = df["import_price"].astype(float)
    df["selling_price"] = df["selling_price"].astype(float)
    df["promotion_price"] = df["promotion_price"].astype(float)
    df["rating"] = df["rating"].astype(float)
    df["sold"] = df["sold"].astype(int)
    df["stock"] = df["stock"].astype(int)

    return df
