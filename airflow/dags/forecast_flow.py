from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

# Import Cosmos components
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior

from etl_app.scripts.crawl_weather import extract_weather_forecast
from etl_app.scripts.crawl_aq import extract_aq_forecast

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
    dag_id="etl_forecast",
    description="ELT Weather & Air Quality – chạy hàng ngày với TaskGroup forecast_flow",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["elt", "pandas", "postgres", "cosmos"],
) as dag:
    # 2. Pipeline Summary Function
    def log_pipeline_summary(**context):
        """Log summary metrics từ forecast crawl tasks"""
        ti = context["ti"]

        # Get XCom từ crawl tasks
        weather_stats = ti.xcom_pull(task_ids="crawl_weather_forecast")
        aq_stats = ti.xcom_pull(task_ids="crawl_aq_forecast")

        logger.info("=" * 60)
        logger.info("📊 PIPELINE SUMMARY - ETL DAY (FORECAST)")
        logger.info("=" * 60)

        if weather_stats:
            logger.info(
                f"🌤️  Weather Forecast: {weather_stats['records_inserted']} records → {weather_stats['table']}"
            )
            logger.info(
                f"   └─ Duration: {weather_stats['duration_seconds']}s, Failed: {weather_stats['failed_records']}"
            )

        if aq_stats:
            logger.info(
                f"💨 AQ Forecast: {aq_stats['records_inserted']} records → {aq_stats['table']}"
            )
            logger.info(
                f"   └─ Duration: {aq_stats['duration_seconds']}s, Failed: {aq_stats['failed_records']}"
            )

        total_records = (weather_stats["records_inserted"] if weather_stats else 0) + (
            aq_stats["records_inserted"] if aq_stats else 0
        )
        logger.info(f"\n✅ Total forecast records: {total_records}")
        logger.info("=" * 60)

    # 3. Crawl tasks
    t_crawl_weather_forecast = PythonOperator(
        task_id="crawl_weather_forecast",
        python_callable=extract_weather_forecast,
        provide_context=True,
    )
    t_crawl_aq_forecast = PythonOperator(
        task_id="crawl_aq_forecast",
        python_callable=extract_aq_forecast,
        provide_context=True,
    )

    # 4. Pipeline summary task
    t_summary = PythonOperator(
        task_id="log_pipeline_summary",
        python_callable=log_pipeline_summary,
        provide_context=True,
    )

    # 3. Định nghĩa DbtTaskGroup (Transformation)
    t_dbt_transformation = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=ProjectConfig("/opt/dbt"),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["tag:forecast_flow"],
            test_behavior=TestBehavior.AFTER_ALL,
            dbt_deps=True,
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": False,
            "vars": {
                # Cộng timedelta xong mới .strftime()
                "is_backfill": "{{ dag_run.conf.get('is_backfill', false) }}",
                "start_date": "{{ (data_interval_start + macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
                "end_date": "{{ (data_interval_end + macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            },
        },
    )

    # 5. Thiết lập luồng phụ thuộc
    [t_crawl_weather_forecast, t_crawl_aq_forecast] >> t_summary >> t_dbt_transformation
