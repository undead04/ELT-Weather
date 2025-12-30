from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from elt_app.scripts.extract.crawl_weather import extract_weather
from elt_app.scripts.transform.transform_weather import transform_weather
from elt_app.scripts.load.load_weather import load_weather
from elt_app.scripts.extract.crawl_aq import extract_aq
from elt_app.scripts.transform.transform_aq import transform_aq
from elt_app.scripts.load.load_aq import load_aq


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['antran.261004@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='etl_day',
    description='ELT Weather & Air Quality – chạy hàng ngày với 2 nhánh song song',
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["elt", "pandas", "postgres"],
    template_searchpath=['/opt/airflow/'],
) as dag:

    t_crawl_weather = PythonOperator(
        task_id="crawl_weather", 
        python_callable=extract_weather,
        provide_context=True
    )
    t_crawl_aq = PythonOperator(
        task_id="crawl_aq", 
        python_callable=extract_aq,
        provide_context=True
    )

    t_transform_weather = SparkSubmitOperator(
        task_id="transform_weather",
        conn_id="spark_default",
        application="/opt/airflow/elt_app/scripts/transform/transform_weather.py",
        name="transform_weather",
        verbose=1,
    )
    t_transform_aq = SparkSubmitOperator(
        task_id="transform_aq",
        conn_id="spark_default",
        application="/opt/airflow/elt_app/scripts/transform/transform_aq.py",
        name="transform_aq",
        verbose=1
    )

    t_load_weather = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather
    )

    t_load_aq = PythonOperator(
        task_id="load_aq", python_callable=load_aq
    )

    t_merge_fact_weather = PostgresOperator(
        task_id="merge_fact_weather",
        split_statements=True,
        postgres_conn_id="postgres_default", # Nhớ tạo Connection này trong Airflow UI
        sql="elt_app/sql/merge_fact_weather.sql",      # Đường dẫn tới file SQL
    )
    t_merge_fact_aq = PostgresOperator(
        task_id="merge_fact_aq",
        split_statements=True,
        postgres_conn_id="postgres_default", # Nhớ tạo Connection này trong Airflow UI
        sql="elt_app/sql/merge_fact_aq.sql",      # Đường dẫn tới file SQL
    )
    # Cho 2 task crawl chạy trước (song song cho nhanh vì nhẹ)
    [t_crawl_weather, t_crawl_aq] >> t_transform_weather >> t_load_weather >> t_merge_fact_weather

    # Sau đó chạy dây chuyền Spark để bảo vệ RAM 4GB
    t_transform_weather >> t_transform_aq >> t_load_aq >> t_merge_fact_aq