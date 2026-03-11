from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
from airflow.operators.python import PythonOperator
from etl_app.scripts.crawl_weather import extract_weather_current
from etl_app.scripts.crawl_aq import extract_aq_current
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email": ["antran.261004@gmail.com"],
    "email_on_failure": True,
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
    dag_id="etl_current",
    description="ETL Weather & Air Quality – chạy hàng phút với TaskGroup current_flow",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["elt", "pandas", "postgres"],
    template_searchpath=["/opt/airflow/"],
) as dag:
    # 2. Pipeline Summary Function
    def log_pipeline_summary(**context):
        """Log summary metrics từ các extract tasks"""
        ti = context["ti"]

        # Get XCom từ extract tasks
        weather_stats = ti.xcom_pull(task_ids="extract_tasks.extract_weather_current")
        aq_stats = ti.xcom_pull(task_ids="extract_tasks.extract_aq_current")

        logger.info("=" * 60)
        logger.info("📊 PIPELINE SUMMARY - ETL MINUTE")
        logger.info("=" * 60)

        if weather_stats:
            logger.info(
                f"🌤️  Weather: {weather_stats['records_inserted']} records → {weather_stats['table']}"
            )
            logger.info(
                f"   └─ Duration: {weather_stats['duration_seconds']}s, Failed: {weather_stats['failed_records']}"
            )

        if aq_stats:
            logger.info(
                f"💨 Air Quality: {aq_stats['records_inserted']} records → {aq_stats['table']}"
            )
            logger.info(
                f"   └─ Duration: {aq_stats['duration_seconds']}s, Failed: {aq_stats['failed_records']}"
            )

        total_records = (weather_stats["records_inserted"] if weather_stats else 0) + (
            aq_stats["records_inserted"] if aq_stats else 0
        )
        logger.info(f"\n✅ Total raw records: {total_records}")
        logger.info("=" * 60)

    # 3. TẦNG EXTRACT (Python) trong TaskGroup
    with TaskGroup(group_id="extract_tasks") as extract_group:
        task_extract_weather_current = PythonOperator(
            task_id="extract_weather_current",
            python_callable=extract_weather_current,
            provide_context=True,
        )
        task_extract_aq_current = PythonOperator(
            task_id="extract_aq_current",
            python_callable=extract_aq_current,
            provide_context=True,
        )

    # 3. Định nghĩa DbtTaskGroup (Transformation)
    t_dbt_transformation = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=ProjectConfig("/opt/dbt"),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["tag:current_flow"],
            test_behavior=TestBehavior.AFTER_ALL,
            dbt_deps=True,
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": False,
            "vars": {
                "start_date": "{{ (data_interval_start + macros.timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S') }}",
                "end_date": "{{ (data_interval_end + macros.timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S') }}",
            },
        },
    )

    # 4. Pipeline summary task
    t_summary = PythonOperator(
        task_id="log_pipeline_summary",
        python_callable=log_pipeline_summary,
        provide_context=True,
    )

    # 5. Thiết lập luồng phụ thuộc
    extract_group >> t_summary >> t_dbt_transformation
