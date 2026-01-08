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
    parquet_s3_path = f"s3a://{BUCKET_NAME}/silver/dim_date/event_date={year}/"

    # Cấu hình kết nối Postgres (Dùng tên service 'postgres' như trong compose)
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Đang đọc dữ liệu từ {parquet_s3_path} bằng Pandas")
        
        # Đọc dữ liệu từ S3 (Pandas sử dụng s3fs/pyarrow ngầm định)
        df = pd.read_parquet(parquet_s3_path)
        if df.empty:
            logger.error("No data to load")
            raise
        # Log thông tin dữ liệu để kiểm tra
        logger.info(f"Columns found: {df.columns.tolist()}")

        # =========================
        # LOAD INTO POSTGRES
        # =========================

        table = "stg_dim_date"
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
        logger.error(f"Failed loading {table}: {e}")
        raise
    finally:
        logger.info("Process finished")

if __name__ == "__main__":
    setup_logging()
    load_date()