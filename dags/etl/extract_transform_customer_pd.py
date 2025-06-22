import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def extract_transform_customer_pd(offset, batch_size, conn):
    logger.info("Extract dim_customer")

    query = f"""
        SELECT
            c.id AS client_id,
            c.name as name,
            c.email as email,
            c.phone as phone,
            c.address as address,
            COALESCE(SUM(o.total_amount), 0) AS total_spent,
            COALESCE(COUNT(o.id), 0) AS num_order
        FROM client c
        JOIN orders o ON c.id = o.client_id
        GROUP BY c.id, c.name, c.email, c.phone, c.address
        OFFSET {offset} LIMIT {batch_size}
    """

    df = pd.read_sql_query(query, conn)
    logger.info("Transform dim_customer")
    df = transform_customer(df)
    logger.info("Extract_transform success")
    return df


def transform_customer(df: pd.DataFrame) -> pd.DataFrame:
    df["email"] = df["email"].astype(str).str.strip()
    df["phone"] = df["phone"].astype(str).str.strip()
    df["address"] = df["address"].astype(str).str.strip()

    df["email"] = df["email"].replace("", "N/A")
    df["phone"] = df["phone"].replace("", "N/A")
    df["address"] = df["address"].replace("", "N/A")

    df["num_order"] = df["num_order"].fillna(0).astype(int)
    df["total_spent"] = df["total_spent"].fillna(0.0)
    df["total_spent"] = np.where(df["total_spent"] < 0, 0.0, df["total_spent"])
    df["num_order"] = np.where(df["num_order"] < 0, 0, df["num_order"])

    return df