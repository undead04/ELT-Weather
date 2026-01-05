import pandas as pd
from sqlalchemy import create_engine
from elt_app.utils.logging import get_logger, setup_logging
from elt_app.utils.config import (
    POSTGRES_CONN_URI
)
from elt_app.utils.utils import get_last_file_s3

logger = get_logger("aq.log")

def load_aq(** context):
    target_date = context.get("ds")
    # Lấy file Air Quality mới nhất từ S3
    input_path = get_last_file_s3(f"silver/fact_aq/event_date={target_date}", ".parquet")

    if input_path is None:
        logger.error("Không tìm thấy file parquet nào cho Air Quality")
        raise ValueError("Không tìm thấy file parquet nào cho Air Quality")

    # Kết nối tới Postgres (Host là 'postgres' theo Docker service)
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Đang đọc dữ liệu AQ từ {input_path} bằng Pandas")
        
        # Đọc dữ liệu trực tiếp từ S3
        df = pd.read_parquet(input_path)
        
        staging_table = "stg_fact_air_quality"
        logger.info(f"Đang nạp {len(df)} dòng vào bảng: {staging_table}")

        # Ghi dữ liệu vào Postgres (overwrite bảng staging)
        df.to_sql(
            name=staging_table,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=5000
        )

        logger.info(f"Ghi dữ liệu vào {staging_table} thành công")
    
    except Exception as e:
        logger.error(f"Lỗi khi nạp staging Air Quality: {e}")
        raise
    finally:
        logger.info("Hoàn thành tiến trình load_aq")

if __name__ == "__main__":
    setup_logging()
    load_aq()