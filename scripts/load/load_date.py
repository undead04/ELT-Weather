
from pathlib import Path
import duckdb
from datetime import datetime
import pandas as pd 
BASE_DIR = Path.cwd()

def get_last_parquet_file(directory: Path):
    parquet_files = list(directory.glob("date_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError("No parquet files found in the specified directory.")
    return max(parquet_files, key=lambda f: f.stat().st_mtime)

def load_date_DW():
    # Path đến DuckDB
    database_path = Path.cwd() / "sql/weather.duckdb"
    conn = duckdb.connect(str(database_path))

    # Lấy parquet mới nhất
    parquet_path = get_last_parquet_file(Path.cwd() / "data/processed/date")

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
    print("dim_date loaded into DuckDB successfully.")    
    conn.close()

if __name__ == "__main__":
    load_date_DW()
