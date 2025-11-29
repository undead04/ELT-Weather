import duckdb
from pathlib import Path
import pandas as pd
import datetime
def get_last_file():
    path = Path.cwd()  / "data" / "processed" / "city" / "city_*.parquet"
    files = list(path.parent.glob(path.name))
    if not files:
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return latest_file

def load_city():
    input_path = get_last_file()
    if input_path is None:
        print("No processed city data file found.")
        return
    # Lưu vào DuckDB
    database_path = Path.cwd() / "sql/weather.duckdb"
    conn = duckdb.connect(str(database_path))
    conn.execute(f"""
        INSERT INTO DIM_CITY (city_id, city_name, lat, lon, country)
        SELECT 
            city_id,
            city_name,
            lat,
            lon,
            country
        FROM '{input_path.as_posix()}'
        ON CONFLICT (city_id) DO NOTHING ;
            """)
    # Close DuckDB connection
    conn.close()
    print(f"Saved dim_city to Parquet at {input_path}")

if __name__ == "__main__":
    load_city()