
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator # Task chạy SQL
from elt_app.scripts.extract.crawl_city import crawl_city
from elt_app.scripts.extract.generate_date import generate_data_date
from elt_app.scripts.extract.generate_time import generate_data_time
from elt_app.scripts.load.load_date import load_date
from elt_app.scripts.load.load_time import load_time
from elt_app.scripts.load.load_city import load_city
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email": ['antran.261004@gmail.com'],
    "email_on_failure": True,
}

with DAG(
    dag_id="etl_year",
    description="ETL Star Schema using Pandas and Postgres SQL",
    default_args=default_args,
    schedule_interval="0 0 1 1 *",
    start_date=days_ago(1),
    catchup=False,
    tags=["elt", "pandas", "postgres"],
    template_searchpath=['/opt/airflow/'],
) as dag:

    # --- TẦNG EXTRACT (Python) ---
    task_generate_date = PythonOperator(task_id="generate_date",
     python_callable=generate_data_date,
     provide_context=True)
    task_generate_time = PythonOperator(task_id="generate_time",
     python_callable=generate_data_time,
     provide_context=True)
    task_crawl_city = PythonOperator(task_id="crawl_city",
     provide_context=True,
     python_callable=crawl_city, op_kwargs={"year": "2024"})
    # Tầng Transform (Spark)
    task_transform_city = SparkSubmitOperator(
        task_id="transform_city", 
        application="/opt/airflow/elt_app/scripts/transform/transform_city.py",
        conn_id="spark_default",
        name="transform_city",
        verbose=1
    )
    # --- TẦNG LOAD (Pandas + SQLAlchemy) ---
    # Đã thay thế SparkSubmitOperator bằng PythonOperator cho nhẹ máy
    task_load_date = PythonOperator(task_id="load_date", python_callable=load_date,
    provide_context=True)
    task_load_time = PythonOperator(task_id="load_time", python_callable=load_time,
    provide_context=True)
    task_load_city = PythonOperator(task_id="load_city", python_callable=load_city,
    provide_context=True)

    # --- TẦNG TRANSFORM (SQL - Hợp nhất Star Schema) ---
    # Task này chạy file SQL để Join và Merge dữ liệu từ stg_ vào Gold Layer
    task_merge_dim_city = PostgresOperator(
        task_id="merge_dim_city",
        split_statements=True,
        postgres_conn_id="postgres_default", # Nhớ tạo Connection này trong Airflow UI
        sql="elt_app/sql/merge_dim_city.sql",      # Đường dẫn tới file SQL
    )
    task_merge_dim_date = PostgresOperator(
        task_id="merge_dim_date",
        split_statements=True,
        postgres_conn_id="postgres_default", # Nhớ tạo Connection này trong Airflow UI
        sql="elt_app/sql/merge_dim_date.sql",      # Đường dẫn tới file SQL
    )
    task_merge_dim_time = PostgresOperator(
        task_id="merge_dim_time",
        split_statements=True,
        postgres_conn_id="postgres_default", # Nhớ tạo Connection này trong Airflow UI
        sql="elt_app/sql/merge_dim_time.sql",      # Đường dẫn tới file SQL
    )

    # --- THIẾT LẬP LUỒNG CHẠY (DAG FLOW) ---
    # 1. Tạo dữ liệu thô
    # 2. Load vào các bảng Staging (Bronze/Silver)
    # 3. Chạy SQL để ra Star Schema (Gold)
    
    task_generate_date >> task_load_date >> task_merge_dim_date
    task_generate_time >> task_load_time >> task_merge_dim_time
    task_crawl_city >> task_transform_city >> task_load_city >> task_merge_dim_city