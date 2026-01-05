import pandas as pd
from sqlalchemy import create_engine
from elt_app.utils.config import (
    POSTGRES_CONN_URI,
    BUCKET_NAME
)
from elt_app.utils.logging import get_logger,setup_logging
from elt_app.utils.utils import get_last_file_s3
logger = get_logger("time.log")

def load_time(** context):
    target_date = context['logical_date']
    year = target_date.year
    parquet_s3_path = get_last_file_s3(f"silver/dim_time/",".parquet")

    if parquet_s3_path is None:
        logger.error("No silver time parquet found in S3 → abort load")
        raise ValueError("Silver time data not found")

    # Lưu ý: 'postgres' là tên service database trong docker-compose của bạn
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Reading parquet from {parquet_s3_path} using Pandas")
        # Đọc dữ liệu (Pandas hỗ trợ đọc trực tiếp từ S3 nếu có s3fs)
        df = pd.read_parquet(parquet_s3_path)
        if df.empty:
            logger.warning("No data found in parquet file")
            raise ValueError("Empty silver time data")
        # =========================
        # 2. LOAD INTO POSTGRES (Staging)
        table = "stg_dim_time"
        # =========================
        logger.info(f"Writing data into {table} using SQLAlchemy")
        
        # if_exists='replace' tương đương với .mode("overwrite")
        df.to_sql(
            name=table, 
            con=engine, 
            if_exists='replace', 
            index=False,
            method='multi', # Tối ưu hóa tốc độ insert
        )

        logger.info(f"Data written to {table} successfully")

    except Exception as e:
        logger.error(f"Failed loading stg_dim_time: {e}")
        raise
    finally:
        logger.info("Process finished")

if __name__ == "__main__":
    setup_logging()
    load_time()