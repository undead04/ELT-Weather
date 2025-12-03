
from pathlib import Path
from utils.config import PROCESSED_DIR, SQL_DB_PATH
from utils.logging import get_logger
import duckdb 

def get_last_parquet_file(directory: Path):
    parquet_files = list(directory.glob("date_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError("No parquet files found in the specified directory.")
    return max(parquet_files, key=lambda f: f.stat().st_mtime)

def load_date_DW():
    # Path đến DuckDB
    logger = get_logger(__name__, domain_file="date.log")
    conn = duckdb.connect(str(SQL_DB_PATH))

    # Lấy parquet mới nhất
    parquet_path = get_last_parquet_file(PROCESSED_DIR / "date")

    # Load parquet trực tiếp vào DuckDB
    conn.execute(f"""
        INSERT INTO DIM_DATE (full_date,day,month,year,quarter,is_weekend,day_of_week)
        SELECT 
            full_date,
            day,
            month,
            year,
            quarter,
            is_weekend,
            day_of_week
        FROM '{parquet_path.as_posix()}'
        ON CONFLICT (full_date) DO NOTHING ;
            """)
    logger.info("dim_date loaded into DuckDB successfully.")    
    conn.close()

if __name__ == "__main__":
    from utils.logging import setup_logging

    setup_logging()
    load_date_DW()
