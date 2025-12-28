from pathlib import Path
import os

# Project root
BASE_DIR: Path = Path(__file__).resolve().parents[1]

LOG_DIR: Path = BASE_DIR / "logs"
# Postgres
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
POSTGRES_DB = os.getenv("POSTGRES_DB", "warehouse")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

EMAIL = os.getenv("AIRFLOW__SMTP__SMTP_USER", "email_nhan_thong_bao@gmail.com")

# Connection String for Psycopg2
POSTGRES_CONN_URI = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

AWS_BUCKET_ACESS_KEY= os.getenv("AWS_ACCESS_KEY_ID")
AWS_BUCKET_SECRET_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")
REGION_NAME=os.getenv("AWS_DEFAULT_REGION")
BUCKET_NAME = os.getenv("BUCKET")

def get_base_dir() -> Path:
    return BASE_DIR


__all__ = [
    "BASE_DIR",
    "LOG_DIR",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_CONN_URI",
    "POSTGRES_JDBC_URL",
    "AWS_BUCKET_ACESS_KEY",
    "AWS_BUCKET_SECRET_KEY",
    "REGION_NAME",
    "BUCKET_NAME",
    "get_base_dir",
    "EMAIL"
]
