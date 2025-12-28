import pandas as pd
from sqlalchemy import create_engine
from elt_app.utils.logging import get_logger, setup_logging
from elt_app.utils.config import (
    POSTGRES_CONN_URI
)
from elt_app.utils.utils import get_last_file_s3

logger = get_logger("weather.log")

def load_weather():
    # Lấy đường dẫn file parquet mới nhất từ S3
    input_path = get_last_file_s3("staging/weather/")

    if input_path is None:  
        logger.error("Không tìm thấy bất kỳ file weather parquet nào")
        return

    # Kết nối trực tiếp tới Postgres (Service name là 'postgres' theo docker-compose)
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Reading parquet từ {input_path} bằng Pandas")
        
        # Đọc dữ liệu (Pandas tự dùng s3fs để kết nối MinIO/S3)
        df = pd.read_parquet(input_path)
        
        # Log schema và vài dòng đầu để debug tương tự spark.printSchema()
        logger.info(f"Columns found: {df.columns.tolist()}")
        
        # Chọn các cột cần thiết
        selected_columns = [
            "city_name", "temperature", "humidity",
            "wind_speed", "precipitation", "weather_code", 
            "cloud_cover", "rain", "wind_direction", 
            "apparent_temperature", "date", "hour"
        ]
        
        df = df[selected_columns]
        staging_table = "stg_fact_weather"
        logger.info(f"Writing {len(df)} rows to staging table: {staging_table}")

        # Ghi vào Postgres
        # method='multi' giúp insert nhanh hơn. chunksize giúp chia nhỏ dữ liệu tránh treo memory.
        df.to_sql(
            name=staging_table,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=5000 
        )

        logger.info(f"Data written to {staging_table} successfully")
    
    except Exception as e:
        logger.error(f"Failed loading staging FACT_WEATHER: {e}")
        raise
    finally:
        logger.info("Hoàn thành tiến trình load_weather")

if __name__ == "__main__":
    setup_logging()
    load_weather()