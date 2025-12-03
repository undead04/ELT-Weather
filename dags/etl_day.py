from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import docker

def run_in_elt_runner(command: str):
    """
    Thực thi lệnh trong container ELT-runner đang chạy
    """
    client = docker.from_env()

    # Lấy container ELT-runner đang chạy
    container = client.containers.get("eltweather-etl-runner-1")

    # Exec command trong container
    exit_code, output = container.exec_run(command)
    
    # Chuyển bytes sang string
    output_str = output.decode("utf-8")
    print(output_str)

    if exit_code != 0:
        raise Exception(f"Command failed with exit code {exit_code}:\n{output_str}")
    
def crawl_weather_task():
    run_in_elt_runner("python /opt/etl/scripts/extract/crawl_weather.py")

def transform_weather_task():
    run_in_elt_runner("python /opt/etl/scripts/transform/transform_weather.py")

def load_weather_task():
    run_in_elt_runner("python /opt/etl/scripts/load/load_weather.py")

def crawl_aq_task():
    run_in_elt_runner("python /opt/etl/scripts/extract/crawl_aq.py")

def transform_aq_task():
    run_in_elt_runner("python /opt/etl/scripts/transform/transform_aq.py")

def load_aq_task():
    run_in_elt_runner("python /opt/etl/scripts/load/load_aq.py")


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
    description='ELT Weather & Air Quality – chạy hàng ngày với 2 nhánh song song',
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    t_crawl_weather = PythonOperator(
        task_id="crawl_weather", python_callable=crawl_weather_task
    )

    t_transform_weather = PythonOperator(
        task_id="transform_weather", python_callable=transform_weather_task
    )

    t_load_weather = PythonOperator(
        task_id="load_weather", python_callable=load_weather_task
    )

    t_crawl_aq = PythonOperator(
        task_id="crawl_aq", python_callable=crawl_aq_task
    )

    t_transform_aq = PythonOperator(
        task_id="transform_aq", python_callable=transform_aq_task
    )

    t_load_aq = PythonOperator(
        task_id="load_aq", python_callable=load_aq_task
    )

    t_crawl_weather >> t_transform_weather >> t_load_weather
    t_crawl_weather >> t_crawl_aq >> t_transform_aq >> t_load_aq