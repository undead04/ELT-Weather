import pandas as pd
from pathlib import Path
from utils.config import PROCESSED_DIR, SQL_DB_PATH
from utils.logging import get_logger
import duckdb

def get_last_parquet_file(directory: Path):
    parquet_files = list(directory.glob("time_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError("No parquet files found in the specified directory.")
    return max(parquet_files, key=lambda f: f.stat().st_mtime)

def load_time_db():
    # Path đến DuckDB
    logger = get_logger(__name__, domain_file="time.log")
    conn = duckdb.connect(str(SQL_DB_PATH))

    # Lấy parquet mới nhất
    parquet_path = get_last_parquet_file(PROCESSED_DIR / "time")

    # Load parquet trực tiếp vào DuckDB
    conn.execute(f"""
        INSERT INTO DIM_TIME (hour,minute,second,time_bucket)
        SELECT 
            hour,
            minute,
            second,
            time_bucket 
        FROM '{parquet_path.as_posix()}'
        ON CONFLICT (hour) DO NOTHING
            """)

    logger.info("dim_time loaded into DuckDB successfully.")
    conn.close()

if __name__ == "__main__":
    from utils.logging import setup_logging

    setup_logging()
    load_time_db()
