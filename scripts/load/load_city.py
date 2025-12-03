import duckdb
from pathlib import Path
from utils.config import PROCESSED_DIR, SQL_DB_PATH
from utils.logging import get_logger
import pandas as pd
import datetime
def get_last_file():
    path = PROCESSED_DIR / "city" / "city_*.parquet"
    files = list(path.parent.glob(path.name))
    if not files:
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return latest_file

def load_city():
    input_path = get_last_file()
    logger = get_logger(__name__, domain_file="city.log")
    if input_path is None:
        logger.error("No processed city data file found.")
        return
    
    # Lưu vào DuckDB
    conn = duckdb.connect(str(SQL_DB_PATH))
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
    logger.info("Saved dim_city to Parquet at %s", input_path)

if __name__ == "__main__":
    from utils.logging import setup_logging

    setup_logging()
    load_city()