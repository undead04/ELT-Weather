
from elt_app.utils.logging import get_logger
from elt_app.utils.config import AWS_BUCKET_ACESS_KEY,AWS_BUCKET_SECRET_KEY,REGION_NAME,BUCKET_NAME
import s3fs
from datetime import datetime
import pandas as pd
from elt_app.utils.logging import setup_logging
from elt_app.utils.utils import get_last_file_s3

def get_time_bucket(hour:int):
    if 6 <= hour < 12:
        return "Sáng"
    elif 12 <= hour < 18:
        return "Chiều"
    elif 18 <= hour < 22:
        return "Tối"
    else:
        return "Đêm"

    
def generate_data_time():
    datas = []
    # ---- 1. Kiểm tra nếu file parquet đã tồn tại thì không tạo lại ---
    prefix = "staging/time/"
    bucket = BUCKET_NAME


    logger = get_logger(__name__, domain_file="time.log")

    if get_last_file_s3(prefix) is not None:
        logger.info("Parquet file for dim_time already exists. Skipping generation.")
        return
    # ---- 2. Tạo dữ liệu dim_time ----
    for i in range(0,24):
        datas.append({
            "hour": i,
            "minute": 0,
            "second": 0,
            "time_bucket": get_time_bucket(i)
        })
     # ---- 3. Lưu file ----

    df = pd.DataFrame(datas)
    
    date_str = datetime.now().strftime("%Y-%m-%d")

    key = f"{prefix}time_{date_str}.parquet"

    s3_path = f"s3://{bucket}/{key}"

    # Tạo filesystem S3
    fs = s3fs.S3FileSystem(
        key=AWS_BUCKET_ACESS_KEY,
        secret=AWS_BUCKET_SECRET_KEY,
        client_kwargs={'region_name': REGION_NAME}
    )

    # Ghi DataFrame thành Parquet lên S3
    df.to_parquet(s3_path, engine="pyarrow", index=False, filesystem=fs)
    logger.info("Loaded dim_time into Parquet at %s", key)
    
if __name__ == "__main__":
    setup_logging()
    generate_data_time()