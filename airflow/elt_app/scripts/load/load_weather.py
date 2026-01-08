import pandas as pd
from sqlalchemy import create_engine
from elt_app.utils.logging import get_logger, setup_logging
from elt_app.utils.config import (
    POSTGRES_CONN_URI,BUCKET_NAME
)
from elt_app.utils.utils import get_last_file_s3

logger = get_logger("weather.log")

def load_weather(** context):
    target_date = context.get("ds")
    # Lấy đường dẫn file parquet mới nhất từ S3
    input_path = f"s3a://{BUCKET_NAME}/silver/fact_weather/event_date={target_date}"


    # Kết nối trực tiếp tới Postgres (Service name là 'postgres' theo docker-compose)
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Reading parquet từ {input_path} bằng Pandas")
        
        # Đọc dữ liệu (Pandas tự dùng s3fs để kết nối MinIO/S3)
        df = pd.read_parquet(input_path,engine="pyarrow")
        if df.empty:
            logger.warning("Không có dữ liệu để nạp vào Postgres")
            raise
        # Log schema và vài dòng đầu để debug tương tự spark.printSchema()
        logger.info(f"Columns found: {df.columns.tolist()}")
        
        # Chọn các cột cần thiết
        staging_table = "stg_fact_weather"
        
        with engine.begin() as conn:  # engine.begin() tự động START TRANSACTION và COMMIT khi thoát block
            logger.info(f"Đang nạp {len(df)} dòng vào bảng: {staging_table}")

            df.to_sql(
                name=staging_table,
                con=conn, # Dùng connection thay vì engine
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=5000 
            )

        logger.info(f"Ghi dữ liệu vào {staging_table} thành công và đã COMMIT.")
    
    except Exception as e:
        logger.error(f"Failed loading staging FACT_WEATHER: {e}")
        raise
    finally:
        logger.info("Hoàn thành tiến trình load_weather")

if __name__ == "__main__":
    setup_logging()
    load_weather()