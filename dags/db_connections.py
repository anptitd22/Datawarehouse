import psycopg2
import pyodbc

from airflow.hooks.base import BaseHook
def get_pg_conn():
    conn = BaseHook.get_connection("postgres_default")
    return psycopg2.connect(
        dbname="postgres",
        user="admin",
        password="123456",
        host="postgres_container",
        port="5432"
    )
def get_sqlserver_conn():
    conn = BaseHook.get_connection('sqlserver_default')
    return pyodbc.connect(
        f"DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER=host.docker.internal,1433;"
        f"DATABASE=datamart_db;"
        f"UID=sa;"
        f"PWD=an147258;"
        'TrustServerCertificate=yes;'
        'Connection Timeout=30;'
    )

# def run_full_etl():
#     # dim
#     load_date(transform_dates(extract_dates()))
#     load_customer(transform_customer(extract_customer()))
#     load_product(transform_product(extract_product()))
#     load_order(transform_order(extract_order()))
#
#     # Fact tables
#     load_fact_sales(transform_fact_sales(extract_fact_sales(), get_dimension_mappings()))
#     load_fact_status(transform_fact_status(extract_fact_status(), get_status_dimension_mappings()))
#
# full_etl_task = PythonOperator(
#     task_id='run_full_etl',
#     python_callable=run_full_etl,
#     dag=dag
# )