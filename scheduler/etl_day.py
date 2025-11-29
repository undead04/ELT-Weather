from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from scripts.extract.crawl_weather import extract_weather
from scripts.extract.crawl_aq import extract_aq

from scripts.transform.transform_weather import transform_weather
from scripts.transform.transform_aq import transform_aq

from scripts.load.load_weather import load_weather
from scripts.load.load_aq import load_aq


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['antran.261004@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='etl_day',
    default_args=default_args,
    description='ELT Weather & Air Quality – chạy hàng ngày với 2 nhánh song song',
    schedule_interval="@daily",    # chạy mỗi ngày 1 lần
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # ====================
    # WEATHER PIPELINE
    # ====================
    crawl_weather_task = PythonOperator(
        task_id='crawl_weather',
        python_callable=extract_weather,
    )

    transform_weather_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather,
    )

    load_weather_task = PythonOperator(
        task_id='load_weather',
        python_callable=load_weather,
    )


    # ====================
    # AIR QUALITY PIPELINE
    # ====================
    crawl_aq_task = PythonOperator(
        task_id='crawl_aq',
        python_callable=extract_aq,
    )

    transform_aq_task = PythonOperator(
        task_id='transform_aq',
        python_callable=transform_aq,
    )

    load_aq_task = PythonOperator(
        task_id='load_aq',
        python_callable=load_aq,
    )


    # ====================
    # DEFINE PARALLEL FLOWS
    # ====================
    crawl_weather_task >> transform_weather_task >> load_weather_task
    crawl_aq_task >> transform_aq_task >> load_aq_task
