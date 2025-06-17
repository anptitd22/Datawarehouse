from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from etl_dim_customer import extract_customer, transform_customer, load_customer, send_customer_to_kafka
from etl_dim_product import extract_product, transform_product, load_product
from etl_dim_date import extract_dates, transform_dates, load_date
from etl_fact_sales import extract_fact_sales, load_fact_sales, transform_fact_sales, get_dimension_mappings
from etl_dim_order import extract_order, transform_order, load_order
from etl_fact_status import extract_fact_status, load_fact_status, transform_fact_status, get_status_dimension_mappings

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for data warehouse',
    schedule='*/10 * * * *',  # Chạy hàng ngày
    catchup=False
) as dag:
    # ============================ DIMENSION TABLES ============================
    with TaskGroup("dim_date_group") as dim_date_group:
        extract_dates_task = PythonOperator(task_id='extract', python_callable=extract_dates)
        transform_dates_task = PythonOperator(task_id='transform',
                                              python_callable=lambda: transform_dates(extract_dates()))
        load_date_task = PythonOperator(task_id='load',
                                         python_callable=lambda: load_date(transform_dates(extract_dates())))
        extract_dates_task >> transform_dates_task >> load_date_task

    with TaskGroup("dim_customer_group") as dim_customer_group:
        extract_customer_task = PythonOperator(task_id='extract', python_callable=extract_customer)
        transform_customer_task = PythonOperator(task_id='transform',
                                                 python_callable=lambda: transform_customer(extract_customer()))
        send_customer_to_kafka_task = PythonOperator(
            task_id='send_to_kafka',
            python_callable=lambda: send_customer_to_kafka(transform_customer(extract_customer()))  # <-- hàm producer bạn đã có
        )

        load_customer_task = PythonOperator(task_id='load', python_callable=load_customer)
        extract_customer_task >> transform_customer_task >> send_customer_to_kafka_task >> load_customer_task

    with TaskGroup("dim_product_group") as dim_product_group:
        extract_product_task = PythonOperator(task_id='extract', python_callable=extract_product)
        transform_product_task = PythonOperator(task_id='transform',
                                                python_callable=lambda: transform_product(extract_product()))
        load_product_task = PythonOperator(task_id='load',
                                           python_callable=lambda: load_product(transform_product(extract_product())))
        extract_product_task >> transform_product_task >> load_product_task

    with TaskGroup("dim_order_group") as dim_order_group:
        extract_order_task = PythonOperator(task_id='extract', python_callable=extract_order)
        transform_order_task = PythonOperator(task_id='transform',
                                              python_callable=lambda: transform_order(extract_order()))
        load_order_task = PythonOperator(task_id='load',
                                         python_callable=lambda: load_order(transform_order(extract_order())))
        extract_order_task >> transform_order_task >> load_order_task

    # ============================ FACT SALES TABLE ============================
    with TaskGroup("fact_sales_group") as fact_sales_group:
        extract_fact_sales_task = PythonOperator(task_id='extract', python_callable=extract_fact_sales)
        transform_fact_sales_task = PythonOperator(
            task_id='transform',
            python_callable=lambda: transform_fact_sales(extract_fact_sales(), get_dimension_mappings())
        )
        load_fact_sales_task = PythonOperator(
            task_id='load',
            python_callable=lambda: load_fact_sales(transform_fact_sales(extract_fact_sales(), get_dimension_mappings()))
        )
        extract_fact_sales_task >> transform_fact_sales_task >> load_fact_sales_task

    # ============================ FACT STATUS TABLE ============================
    with TaskGroup("fact_status_group") as fact_status_group:
        extract_fact_status_task = PythonOperator(task_id='extract', python_callable=extract_fact_status)
        transform_fact_status_task = PythonOperator(
            task_id='transform',
            python_callable=lambda: transform_fact_status(extract_fact_status(), get_status_dimension_mappings())
        )
        load_fact_status_task = PythonOperator(
            task_id='load',
            python_callable=lambda: load_fact_status(transform_fact_status(extract_fact_status(), get_status_dimension_mappings()))
        )
        extract_fact_status_task >> transform_fact_status_task >> load_fact_status_task

    # ============================ PIPELINE DEPENDENCIES ============================

    [load_date_task, load_customer_task, load_product_task, load_order_task] >> extract_fact_sales_task
    [load_date_task, load_order_task] >> extract_fact_status_task