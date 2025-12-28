from datetime import datetime,timedelta
import pandas as pd
from pathlib import Path
from elt_app.utils.logging import get_logger
from elt_app.utils.logging import setup_logging
from elt_app.utils.config import AWS_BUCKET_ACESS_KEY,AWS_BUCKET_SECRET_KEY,REGION_NAME,BUCKET_NAME
import s3fs

def generate_data_date():
    logger = get_logger(__name__, domain_file="date.log")
    prefix = "staging/date/"
    bucket = BUCKET_NAME
    
    datas = []
    # Lấy năm trước
    year = datetime.now().year
    # Sinh tất cả ngày trong năm trước
    start_date: datetime = datetime(year, 1, 1)
    end_date: datetime = datetime(year, 12, 31)
    delta = timedelta(days=1)

    current_date = start_date
    i = 1
    while current_date <= end_date:
        datas.append({
            "full_date": current_date,
            "year": current_date.year,
            "month": current_date.month,
            "day": current_date.day,
            "day_of_week": current_date.weekday(),  # 0=Monday, 6=Sunday
            "quarter": (current_date.month - 1) // 3 + 1,
            "is_weekend": True if current_date.weekday() >= 5 else False
        })
        current_date += delta
        i += 1
     # ---- 4. Lưu file ----
    df = pd.DataFrame(datas)

    date_str = datetime.now().strftime("%Y-%m-%d")

    key = f"{prefix}date_{date_str}.parquet"

    s3_path = f"s3://{bucket}/{key}"

    # Tạo filesystem S3
    fs = s3fs.S3FileSystem(
        key=AWS_BUCKET_ACESS_KEY,
        secret=AWS_BUCKET_SECRET_KEY,
        client_kwargs={'region_name': REGION_NAME}
    )

    # Ghi DataFrame thành Parquet lên S3
    df.to_parquet(s3_path, engine="pyarrow",use_deprecated_int96_timestamps=True, index=False, filesystem=fs)
    logger.info("Loaded dim_date into Parquet at %s", key)


if __name__ == "__main__":
    setup_logging()
    generate_data_date()