from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.extract.generate_time import generate_data_time
from scripts.extract.generate_date import generate_data_date
from scripts.extract.crawl_city import crawl_city
from scripts.transform.transform_city import transform_city
from scripts.load.load_city import load_city

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
    dag_id='etl_city',
    default_args=default_args,
    description='ELT city – chạy 1 lần / năm hoặc run thủ công',
    schedule_interval="0 0 1 1 *",   # chạy lúc 00:00 ngày 1 tháng 1 hằng năm
    start_date=days_ago(1),
    catchup=False,
) as dag:

    generate_date_task = PythonOperator(
        task_id='generate_date',
        python_callable=generate_data_date,
    )

    generate_time_task = PythonOperator(
        task_id='generate_time',
        python_callable=generate_data_time,
    )

    crawl_city_task = PythonOperator(
        task_id='crawl_city',
        python_callable=crawl_city,
    )

    transform_city_task = PythonOperator(
        task_id='transform_city',
        python_callable=transform_city,
    )

    load_city_task = PythonOperator(
        task_id='load_city',
        python_callable=load_city,
    )

    # Define ETL pipeline flow
    generate_date_task >> generate_time_task >> crawl_city_task >> transform_city_task >> load_city_task
