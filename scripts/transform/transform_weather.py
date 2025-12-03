# elt_scripts/load/load_duckdb.py
from datetime import datetime
import pandas as pd 
from pathlib import Path
from utils.config import RAW_DIR, PROCESSED_DIR
from utils.logging import get_logger

def get_last_file():
    path = RAW_DIR / "weather" / "weather_*.json"
    files = list(path.parent.glob(path.name))
    if not files:
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return latest_file

def transform_weather():
    # Lấy file dữ liệu thời tiết mới nhất
    input_path = get_last_file()
    logger = get_logger(__name__, domain_file="weather.log")
    if input_path is None:
        logger.error("No weather data folder found.")
        return
    
    # sữ lí dữ liệu
    list_cols = [
            "temperature", "humidity", "wind_speed",
            "weather_code", "precipitation", "cloud_cover", "rain",
            "wind_direction", "apparent_temperature"
        ]
    df = pd.read_json(input_path)
    df.rename(columns={
            "temperature_2m":"temperature",
            "relative_humidity_2m":"humidity",
            "dew_point_2m":"dew_point",
            "wind_speed_10m":"wind_speed",
            "wind_direction_10m":"wind_direction",
            "weather_code":"weather_code",
            "cloud_cover_low":"cloud_cover"
        }, inplace=True)
    df.drop_duplicates(subset=["city_id","city_name"],inplace=True)
    df_exploded = df.explode(list_cols+['time']).reset_index(drop=True)
    df_exploded.fillna({col: 0 for col in list_cols}, inplace=True)
    df_exploded["date"] = pd.to_datetime(df_exploded["time"]).dt.date
    df_exploded["hour"] = pd.to_datetime(df_exploded["time"]).dt.hour

    # Lưu thông tin vào parquet
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_parquet_path = PROCESSED_DIR / "weather" / f"weather_{date_str}.parquet"
    out_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df_exploded.to_parquet(out_parquet_path, engine="pyarrow", index=False)
    logger.info("Saved weather records to %s", out_parquet_path)
    
if __name__ == '__main__':
    from utils.logging import setup_logging

    setup_logging()
    transform_weather()
