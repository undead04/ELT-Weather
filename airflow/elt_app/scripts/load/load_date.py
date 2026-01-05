import pandas as pd
from sqlalchemy import create_engine
from elt_app.utils.config import (
    POSTGRES_CONN_URI,
    BUCKET_NAME
)
from elt_app.utils.logging import get_logger, setup_logging
from elt_app.utils.utils import get_last_file_s3

logger = get_logger("date.log")

def load_date(** context):
    # Lấy đường dẫn file mới nhất từ S3
    target_date = context['logical_date']
    year = target_date.year
    parquet_s3_path = get_last_file_s3(f"silver/dim_date/event_date={year}/",".parquet")

    if parquet_s3_path is None:
        logger.error("No silver date parquet found in S3 → abort load")
        raise ValueError("Silver date data not found")

    # Cấu hình kết nối Postgres (Dùng tên service 'postgres' như trong compose)
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Reading parquet from {parquet_s3_path} using Pandas")
        
        # Đọc dữ liệu từ S3 (Pandas sử dụng s3fs/pyarrow ngầm định)
        df = pd.read_parquet(parquet_s3_path)
        if df.empty:
            logger.error("No data to load")
            raise ValueError("Empty silver date data")
        # Log thông tin dữ liệu để kiểm tra
        logger.info(f"Data shape: {df.shape}")

        # =========================
        # LOAD INTO POSTGRES
        # =========================
        logger.info("Writing data into dim_date using SQLAlchemy")
        table = "stg_dim_date"
        # if_exists='replace' tương đương với .mode("overwrite")
        df.to_sql(
            name=table, 
            con=engine, 
            if_exists='replace', 
            index=False,
            method='multi', # Tăng tốc độ insert hàng loạt
        )

        logger.info(f"Data written to {table} successfully")
    

    except Exception as e:
        logger.error(f"Failed loading {table}: {e}")
        raise
    finally:
        logger.info("Process finished")

if __name__ == "__main__":
    setup_logging()
    load_date()