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