import pandas as pd
from pathlib import Path
from utils.config import RAW_DIR, PROCESSED_DIR
from utils.logging import get_logger
from datetime import datetime
def get_last_file():
    path = RAW_DIR / "city" / "city_*.json"
    files = list(path.parent.glob(path.name))
    if not files:
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return latest_file

logger = get_logger(__name__, domain_file="city.log")


def transform_city():
    input_path = get_last_file()
    if input_path is None:
        logger.error("No city data file found.")
        return
    
    df = pd.read_json(input_path, encoding='utf-8')
    df.dropna(axis=0,how="any",subset=["city_id", "name", "lat", "lon"],inplace=True)
    df.drop_duplicates(subset=["city_id","name","lat","lon"],inplace=True)
    df = df.rename(columns={
        "name": "city_name",
    })
    df_cleared = df[["city_id", "city_name", "lat", "lon"]].copy()
    df_finished = df_cleared
    df_finished["country"] = "Việt Nam"  # tất cả dòng đều là 'Vietnam'
    # Lưu file đã chuyển đổi
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_path = PROCESSED_DIR / "city" / f"city_{date_str}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # ghi DataFrame thành file Parquet
    df_finished.to_parquet(output_path, engine="pyarrow", index=False)
    from utils.logging import get_logger

    logger = get_logger(__name__, domain_file="transform_city.log")
    logger.info("Loaded dim_city from Parquet at %s", output_path)

if __name__ == "__main__":
    from utils.logging import setup_logging

    setup_logging()
    transform_city()