import pandas as pd
from pathlib import Path
import duckdb

def get_last_parquet_file(directory: Path):
    parquet_files = list(directory.glob("time_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError("No parquet files found in the specified directory.")
    return max(parquet_files, key=lambda f: f.stat().st_mtime)

def load_time_db():
    # Path đến DuckDB
    database_path = Path.cwd() / "sql/weather.duckdb"
    conn = duckdb.connect(str(database_path))

    # Lấy parquet mới nhất
    parquet_path = get_last_parquet_file(Path.cwd() / "data/processed/time")

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

    print("dim_time loaded into DuckDB successfully.")
    conn.close()

if __name__ == "__main__":
    load_time_db()
