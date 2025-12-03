from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
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
    

def generate_date_task():
    run_in_elt_runner("python /opt/etl/scripts/extract/generate_date.py")

def generate_time_task():
    run_in_elt_runner("python /opt/etl/scripts/extract/generate_time.py")

def crawl_city_task():
    run_in_elt_runner("python /opt/etl/scripts/extract/crawl_city.py")

def transform_city_task():
    run_in_elt_runner("python /opt/etl/scripts/transform/transform_city.py")

def load_city_task():
    run_in_elt_runner("python /opt/etl/scripts/load/load_city.py")

def load_date_task():
    run_in_elt_runner("python /opt/etl/scripts/load/load_date.py")

def load_time_task():
    run_in_elt_runner("python /opt/etl/scripts/load/load_time.py")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_year",
    description="ETL City chạy trong container ELT-runner với Docker SDK",
    schedule_interval="0 0 1 1 *",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    t_generate_date = PythonOperator(
        task_id="generate_date", python_callable=generate_date_task
    )

    t_load_date = PythonOperator(
        task_id="load_date", python_callable=load_date_task
    )

    t_generate_time = PythonOperator(
        task_id="generate_time", python_callable=generate_time_task
    )

    t_load_time = PythonOperator(
        task_id="load_time", python_callable=load_time_task
    )

    t_crawl_city = PythonOperator(
        task_id="crawl_city", python_callable=crawl_city_task
    )

    t_transform_city = PythonOperator(
        task_id="transform_city", python_callable=transform_city_task
    )

    t_load_city = PythonOperator(
        task_id="load_city", python_callable=load_city_task
    )

    # DAG flow
    t_generate_date >> t_load_date >> t_generate_time >> t_load_time
    t_load_date >> t_crawl_city >> t_transform_city >> t_load_city