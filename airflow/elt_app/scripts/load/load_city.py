import pandas as pd
from sqlalchemy import create_engine
from elt_app.utils.logging import get_logger, setup_logging
from elt_app.utils.config import (
    POSTGRES_CONN_URI,BUCKET_NAME
)
from elt_app.utils.utils import get_last_file_s3

logger = get_logger("city.log")

def load_city():
    # Lấy đường dẫn file city parquet mới nhất từ S3
    input_path = f"s3a://{BUCKET_NAME}/silver/dim_city/"
        
    # Kết nối tới Postgres (Service: postgres, DB: airflow)
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Đang đọc dữ liệu city từ {input_path}")
        
        # Đọc dữ liệu bằng Pandas
        df = pd.read_parquet(input_path)
        if df.empty:
            logger.warning("Không có dữ liệu để nạp vào Postgres")
            raise
        logger.info(f"Columns found: {df.columns.tolist()}")
        # Ghi vào bảng staging
        table = "stg_dim_city"
        with engine.begin() as conn:  # engine.begin() tự động START TRANSACTION và COMMIT khi thoát block
            logger.info(f"Đang ghi {len(df)} dòng vào bảng: {table}")

            df.to_sql(
                name=table,
                con=conn, # Dùng connection thay vì engine
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=5000
            )

        logger.info(f"Ghi dữ liệu vào {table} thành công!")
        
    except Exception as e:
        logger.error(f"Lỗi khi load staging DIM_CITY: {e}")
        raise
    finally:
        logger.info("Hoàn thành tiến trình load_city")

if __name__ == "__main__":
    setup_logging()
    load_city()