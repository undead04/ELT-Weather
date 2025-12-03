# elt_scripts/transform/transform_weather.py
from pathlib import Path
import duckdb
from utils.config import PROCESSED_DIR, SQL_DB_PATH
from utils.logging import get_logger

def get_last_file(path: str) -> Path:
    path:Path = PROCESSED_DIR / Path(path)
    files = list(path.parent.glob(path.name))
    if not files:
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return latest_file

def load_aq():
    input_path = get_last_file("aq/aq_*.parquet")
    logger = get_logger(__name__, domain_file="aq.log")
    if input_path is None:
        logger.error("No processed aq data file found.")
        return
    
    conn = duckdb.connect(str(SQL_DB_PATH))
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
    logger.info("Saved fact_air_quality to dim")
if __name__ =="__main__":
        from utils.logging import setup_logging

        setup_logging()
        load_aq()