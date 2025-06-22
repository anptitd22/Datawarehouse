from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from dags.etl_dim_date import consumer_date_kafka, producer_date_kafka
from dags.etl_dim_customer import consumer_customer_kafka, producer_customer_kafka
from dags.etl_dim_product import producer_product_kafka,consumer_product_kafka
from dags.etl_dim_order import producer_order_kafka, consumer_order_kafka
from dags.etl_fact_sales import producer_sales_kafka, consumer_fact_sales_kafka
from dags.etl_fact_status import producer_status_kafka, consumer_fact_status_kafka
from dags.spark.build_spark import install_spark_dependencies

default_args = {
    'owner': 'datamart_AN',
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
    schedule='*/30 * * * *',  # Chạy hàng ngày
    catchup=False
) as dag:
    # ============================ WARM-UP TASK ============================
    install_dependencies_task = PythonOperator(
        task_id='install_spark_dependencies',
        python_callable=install_spark_dependencies
    )

    # ============================ DIMENSION TABLES ============================
    with TaskGroup("dim_date_group") as dim_date_group:
        extract_transform_dates_task = EmptyOperator(
            task_id="extract_transform"
        )
        producer_kafka_task = PythonOperator(
            task_id='producer_kafka',
            python_callable=producer_date_kafka
        )
        consumer_kafka_task = PythonOperator(
            task_id='consumer_kafka',
            python_callable=consumer_date_kafka
        )
        load_date_task = EmptyOperator(
            task_id='load'
        )

        extract_transform_dates_task >> producer_kafka_task >> consumer_kafka_task >> load_date_task

    with TaskGroup("dim_customer_group") as dim_customer_group:
        extract_transform_customer_task = EmptyOperator(
            task_id="extract_transform"
        )
        producer_kafka_task = PythonOperator(
            task_id='producer_kafka',
            python_callable=producer_customer_kafka
        )
        consumer_kafka_task = PythonOperator(
            task_id='consumer_kafka',
            python_callable=consumer_customer_kafka
        )
        load_customer_task = EmptyOperator(
            task_id="load"
        )

        extract_transform_customer_task >> producer_kafka_task >> consumer_kafka_task >> load_customer_task

    with TaskGroup("dim_product_group") as dim_product_group:
        extract_transform_product_task = EmptyOperator(
            task_id='extract_transform'
        )
        producer_kafka_task = PythonOperator(
            task_id='producer_kafka',
            python_callable=producer_product_kafka
        )
        consumer_kafka_task = PythonOperator(
            task_id='consumer_kafka',
            python_callable=consumer_product_kafka
        )
        load_product_task = EmptyOperator(
            task_id='load',
        )
        extract_transform_product_task >> producer_kafka_task >> consumer_kafka_task >> load_product_task

    with TaskGroup("dim_order_group") as dim_order_group:
        extract_transform_order_task = EmptyOperator(
            task_id='extract_transform'
        )
        producer_kafka_task = PythonOperator(
            task_id='producer_kafka',
            python_callable=producer_order_kafka
        )
        consumer_kafka_task = PythonOperator(
            task_id='consumer_kafka',
            python_callable=consumer_order_kafka
        )
        load_order_task = EmptyOperator(
            task_id='load'
        )

        extract_transform_order_task >> producer_kafka_task >> consumer_kafka_task >> load_order_task

    # ============================ FACT SALES TABLE ============================
    with TaskGroup("fact_sales_group") as fact_sales_group:
        extract_transform_sales_task = EmptyOperator(
            task_id='extract_transform'
        )
        producer_kafka_task = PythonOperator(
            task_id='producer_kafka',
            python_callable= producer_sales_kafka
        )
        consumer_kafka_task = PythonOperator(
            task_id='consumer_kafka',
            python_callable=consumer_fact_sales_kafka
        )
        load_fact_sales_task = EmptyOperator(
            task_id='load'
        )
        extract_transform_sales_task >> producer_kafka_task >> consumer_kafka_task >> load_fact_sales_task

    # ============================ FACT STATUS TABLE ============================
    with TaskGroup("fact_status_group") as fact_status_group:
        extract_transform_status_task = EmptyOperator(
            task_id='extract_transform'
        )
        producer_kafka_task = PythonOperator(
            task_id='producer_kafka',
            python_callable= producer_status_kafka
        )
        consumer_kafka_task = PythonOperator(
            task_id='consumer_kafka',
            python_callable=consumer_fact_status_kafka
        )
        load_fact_status_task = EmptyOperator(
            task_id='load'
        )
        extract_transform_status_task >> producer_kafka_task >> consumer_kafka_task >> load_fact_status_task

    # ============================ PIPELINE DEPENDENCIES ============================

    # install_dependencies_task >> [
    #     dim_date_group,
    #     dim_customer_group,
    #     dim_product_group,
    #     dim_order_group
    # ]
    # [load_date_task, load_customer_task, load_product_task, load_order_task] >> extract_transform_sales_task
    # [load_date_task, load_order_task] >> extract_transform_status_task

    install_dependencies_task >> [dim_date_group, dim_customer_group, dim_product_group] >> dim_order_group >> fact_status_group >> fact_sales_group
