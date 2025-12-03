from datetime import datetime,timedelta
import pandas as pd
from pathlib import Path
from utils.logging import get_logger
from utils.logging import setup_logging
from utils.config import PROCESSED_DIR
def generate_data_date():
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
            "is_weekend": 1 if current_date.weekday() >= 5 else 0
        })
        current_date += delta
        i += 1
     # ---- 4. Lưu file ----
    date_str = datetime.now().strftime("%Y%m%d")
    output_path = PROCESSED_DIR / "date" / f"date_{date_str}.parquet"
    logger = get_logger(__name__, domain_file="date.log")
    df = pd.DataFrame(datas)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    logger.info("Loaded dim_date into Parquet at %s", output_path)


if __name__ == "__main__":
    setup_logging()
    generate_data_date()