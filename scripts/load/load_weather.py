from pathlib import Path
import pandas as pd
import duckdb
from utils.config import PROCESSED_DIR, SQL_DB_PATH
from utils.logging import get_logger
def get_last_file():
    path = PROCESSED_DIR / "weather" / "weather_*.parquet"
    files = list(path.parent.glob(path.name))
    if not files:
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return latest_file

def load_weather():

    input_path = get_last_file()
    logger = get_logger(__name__, domain_file="weather.log")
    if input_path is None :
        logger.error("Không tìm thấy bất kỳ file data weather nào")
        return

    conn = duckdb.connect(str(SQL_DB_PATH))
    conn.execute(f"""
        INSERT INTO fact_weather (
                 city_id,
                 date_id,
                 time_id,
                 temperature,
                 humidity,
                 wind_speed,
                 precipitation,
                 weather_code,
                 cloud_cover,
                 rain,
                 wind_direction,
                 apparent_temperature)
        SELECT 
            city_id,
            d.date_id,
            t.time_id,
            temperature,
            humidity,
            wind_speed,
            precipitation,
            weather_code,
            cloud_cover,
            rain,
            wind_direction,
            apparent_temperature
        FROM '{input_path.as_posix()}' i
        JOIN dim_date d on i.date = d.full_date
        JOIN dim_time t on t.hour = i.hour
            """)
    # Close DuckDB connection
    conn.close()
    logger.info("Saved fact_weather to Parquet at %s", input_path)
if __name__ == "__main__":
    from utils.logging import setup_logging

    setup_logging()
    load_weather()