from datetime import datetime,timedelta
import pandas as pd
from pathlib import Path
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
    BASE_DIR = Path.cwd()
    date_str = datetime.now().strftime("%Y%m%d")
    output_path = BASE_DIR / "data/processed/date" / f"date_{date_str}.parquet"
    df = pd.DataFrame(datas)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine="fastparquet", index=False)
    print(f"Loaded dim_date into Parquet at {output_path}")


if __name__ == "__main__":
    generate_data_date()