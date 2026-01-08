import pandas as pd
from sqlalchemy import create_engine
from elt_app.utils.logging import get_logger, setup_logging
from elt_app.utils.config import (
    POSTGRES_CONN_URI,
    BUCKET_NAME
)
from elt_app.utils.utils import get_last_file_s3

logger = get_logger("aq.log")

def load_aq(** context):
    target_date = context.get("ds")
    # Lấy file Air Quality mới nhất từ S3
    input_path = f"s3a://{BUCKET_NAME}/silver/fact_aq/event_date={target_date}"



    # Kết nối tới Postgres (Host là 'postgres' theo Docker service)
    db_url = POSTGRES_CONN_URI
    engine = create_engine(db_url)

    try:
        logger.info(f"Đang đọc dữ liệu AQ từ {input_path} bằng Pandas")
        
        # Đọc dữ liệu trực tiếp từ S3
        df = pd.read_parquet(input_path)
        if df.empty:
            logger.warning("Không có dữ liệu để nạp vào Postgres")
            raise
        # Log schema và vài dòng đầu để debug tương tự spark.printSchema()
        logger.info(f"Columns found: {df.columns.tolist()}")
        
        staging_table = "stg_fact_air_quality"

        # Mở connection rõ ràng để kiểm soát Transaction
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
            # Không cần gọi conn.commit() thủ công vì 'with engine.begin()' đã làm việc đó.
            
        logger.info(f"Ghi dữ liệu vào {staging_table} thành công và đã COMMIT.")
    
    except Exception as e:
        logger.error(f"Lỗi khi nạp staging Air Quality: {e}")
        raise
    finally:
        logger.info("Hoàn thành tiến trình load_aq")

if __name__ == "__main__":
    setup_logging()
    load_aq()