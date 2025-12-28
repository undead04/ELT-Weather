import pandas as pd
from sqlalchemy import create_engine
from elt_app.utils.config import (
    POSTGRES_CONN_URI
)
from elt_app.utils.logging import get_logger, setup_logging
from elt_app.utils.utils import get_last_file_s3

logger = get_logger("date.log")

def load_date():
    # Lấy đường dẫn file mới nhất từ S3
    parquet_s3_path = get_last_file_s3("staging/date/")
    
    if parquet_s3_path is None:
        logger.error("No parquet found for DIM_DATE")
        return

    # Cấu hình kết nối Postgres (Dùng tên service 'postgres' như trong compose)
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Reading parquet from {parquet_s3_path} using Pandas")
        
        # Đọc dữ liệu từ S3 (Pandas sử dụng s3fs/pyarrow ngầm định)
        df = pd.read_parquet(parquet_s3_path)
        
        # Log thông tin dữ liệu để kiểm tra
        logger.info(f"Data shape: {df.shape}")

        # =========================
        # LOAD INTO POSTGRES
        # =========================
        logger.info("Writing data into dim_date using SQLAlchemy")
        
        # if_exists='replace' tương đương với .mode("overwrite")
        df.to_sql(
            name='stg_dim_date', 
            con=engine, 
            if_exists='replace', 
            index=False,
            method='multi', # Tăng tốc độ insert hàng loạt
            chunksize=10000 # Chia nhỏ để tránh quá tải RAM
        )

        logger.info("Data written to dim_date successfully")

    except Exception as e:
        logger.error(f"Failed loading dim_date: {e}")
        raise
    finally:
        logger.info("Process finished")

if __name__ == "__main__":
    setup_logging()
    load_date()