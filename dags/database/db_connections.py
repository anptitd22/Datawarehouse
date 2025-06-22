import psycopg2
import pyodbc

from airflow.hooks.base import BaseHook
def get_pg_conn():
    conn = BaseHook.get_connection("postgres_default")
    return psycopg2.connect(
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        host=conn.host, #host=(<local> or <name_container_docker>)
        port=conn.port #port=5432
    )
def get_sqlserver_conn():
    conn = BaseHook.get_connection('sqlserver_default')
    return pyodbc.connect(
        f"DRIVER=ODBC Driver 17 for SQL Server;" 
        f"SERVER={conn.host},{conn.port};" #host=host.docker.internal, port=1433
        f"DATABASE={conn.schema};" 
        f"UID={conn.login};"
        f"PWD={conn.password};"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )