import pandas as pd
import s3fs
from datetime import datetime
from elt_app.utils.logging import get_logger,setup_logging
from elt_app.utils.config import (
    AWS_BUCKET_ACESS_KEY,
    AWS_BUCKET_SECRET_KEY,
    REGION_NAME,
    BUCKET_NAME
)

def get_time_bucket(hour: int):
    if 6 <= hour < 12:
        return "Sáng"
    elif 12 <= hour < 18:
        return "Chiều"
    elif 18 <= hour < 22:
        return "Tối"
    else:
        return "Đêm"


def generate_data_time():
    logger = get_logger(__name__, domain_file="time.log")

    inseget_time = datetime.now()

    data = []
    for h in range(24):
        data.append({
            "hour": h,
            "minute": 0,
            "second": 0,
            "time_bucket": get_time_bucket(h),
            "inseget_time": inseget_time
        })

    df = pd.DataFrame(data)

    s3_path = f"s3://{BUCKET_NAME}/silver/dim_time/dim_time.parquet"

    fs = s3fs.S3FileSystem(
        key=AWS_BUCKET_ACESS_KEY,
        secret=AWS_BUCKET_SECRET_KEY,
        client_kwargs={"region_name": REGION_NAME}
    )

    # overwrite → idempotent
    df.to_parquet(
        s3_path,
        engine="pyarrow",
        use_deprecated_int96_timestamps=True,
        index=False,
        filesystem=fs
    )

    logger.info("Overwrite dim_time successfully at %s", s3_path)
if __name__ == "__main__":
    setup_logging()
    generate_data_time()