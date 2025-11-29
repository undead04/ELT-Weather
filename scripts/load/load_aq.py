# elt_scripts/transform/transform_weather.py
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd
import pandas as pd
import numpy as np

def get_last_file(path: str) -> Path:
    path:Path = Path.cwd() / path
    files = list(path.parent.glob(path.name))
    if not files:
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return latest_file

def load_aq():
    input_path = get_last_file("data/processed/aq/aq_*.parquet")
    if input_path is None:
        print("No processed aq data file found.")
        return
    database_path = Path.cwd() / "sql/weather.duckdb"
    conn = duckdb.connect(str(database_path))
    conn.execute(f"""
        INSERT INTO fact_air_quality(date_id, time_id,city_id,aqi,pm25,pm10,
                 no2,so2,o3,co,co2)
        SELECT 
            d.date_id,
            t.time_id,
            city_id,
            aqi,
            pm25,
            pm10,
            no2,
            so2,
            o3,
            co,
            co2
        FROM '{input_path.as_posix()}' i
        JOIN dim_date d on d.full_date = i.date
        JOIN dim_time t on t.hour = i.hour
            """)
    # Close DuckDB connection
    conn.close()
    print(f"Saved fact_air_quality to dim")
if __name__ =="__main__":
    load_aq()