import pandas as pd
from sqlalchemy import create_engine
from elt_app.utils.config import (
    POSTGRES_CONN_URI
)
from elt_app.utils.logging import get_logger
from elt_app.utils.utils import get_last_file_s3

logger = get_logger("time.log")

def load_time():
    parquet_s3_path = get_last_file_s3("staging/time")
    if parquet_s3_path is None:
        logger.error("No parquet found for DIM_TIME")
        return

    # Lưu ý: 'postgres' là tên service database trong docker-compose của bạn
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Reading parquet from {parquet_s3_path} using Pandas")
        # Đọc dữ liệu (Pandas hỗ trợ đọc trực tiếp từ S3 nếu có s3fs)
        df = pd.read_parquet(parquet_s3_path)
        # =========================
        # 2. LOAD INTO POSTGRES (Staging)
        # =========================
        logger.info("Writing data into dim_time using SQLAlchemy")
        
        # if_exists='replace' tương đương với .mode("overwrite")
        df.to_sql(
            name='stg_dim_time', 
            con=engine, 
            if_exists='replace', 
            index=False,
            method='multi' # Tối ưu hóa tốc độ insert
        )

        logger.info("Data written to dim_time successfully")

    except Exception as e:
        logger.error(f"Failed loading dim_time: {e}")
        raise
    finally:
        logger.info("Process finished")

if __name__ == "__main__":
    load_time()