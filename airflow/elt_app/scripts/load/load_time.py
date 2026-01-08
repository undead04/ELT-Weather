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
    parquet_s3_path = f"s3a://{BUCKET_NAME}/silver/dim_time/"

    # Lưu ý: 'postgres' là tên service database trong docker-compose của bạn
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Đang đọc dữ liệu từ {parquet_s3_path} bằng Pandas")
        # Đọc dữ liệu (Pandas hỗ trợ đọc trực tiếp từ S3 nếu có s3fs)
        df = pd.read_parquet(parquet_s3_path)
        if df.empty:
            logger.warning("No data found in parquet file")
            raise
        # =========================
        # 2. LOAD INTO POSTGRES (Staging)
        table = "stg_dim_time"
        # =========================
        with engine.begin() as conn:
            logger.info(f"Đang ghi {len(df)} dòng vào bảng: {table}")
            df.to_sql(
                name=table, 
                con=conn, 
                if_exists='replace', 
                index=False,
                method='multi', 
                chunksize=5000
            )

        logger.info(f"Ghi dữ liệu vào {table} thành công!")

    except Exception as e:
        logger.error(f"Failed loading stg_dim_time: {e}")
        raise
    finally:
        logger.info("Process finished")

if __name__ == "__main__":
    setup_logging()
    load_time()