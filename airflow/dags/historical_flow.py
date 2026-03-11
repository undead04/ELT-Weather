from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

# Import Cosmos components
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["antran.261004@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# 1. Cấu hình profile cho Cosmos (Dùng chung cho TaskGroup)
profile_config = ProfileConfig(
    profile_name="weather_analytics",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "dev"},
    ),
)

with DAG(
    dag_id="etl_historical",
    description="ELT Weather & Air Quality – chạy hàng ngày với TaskGroup historical_flow",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["elt", "historical_flow", "pandas", "postgres", "cosmos"],
) as dag:
    # 3. Định nghĩa DbtTaskGroup (Transformation)
    t_dbt_transformation = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=ProjectConfig("/opt/dbt"),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["tag:historical_flow"],
            test_behavior=TestBehavior.AFTER_ALL,
            dbt_deps=True,
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": False,
            "vars": {
                "start_date": "{{ data_interval_start.strftime('%Y-%m-%d') }}",
                "end_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
            },
        },
    )
    t_dbt_transformation
