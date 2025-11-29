import json
from pathlib import Path
from datetime import datetime
import pandas as pd
def get_time_bucket(hour:int):
    if 6 <= hour < 12:
        return "Sáng"
    elif 12 <= hour < 18:
        return "Chiều"
    elif 18 <= hour < 22:
        return "Tối"
    else:
        return "Đêm"
    
def is_parquet_file(directory: Path):
    parquet_files = list(directory.glob("time*.parquet"))
    if not parquet_files:
        return False
    return True
    
def generate_data_time():
    datas = []
    BASE_DIR = Path.cwd()
    # ---- 1. Kiểm tra nếu file parquet đã tồn tại thì không tạo lại ---
    output_folder = BASE_DIR / "data/processed/time"
    if is_parquet_file(output_folder):
        print("Parquet file for dim_time already exists. Skipping generation.")
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
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_path = BASE_DIR / "data/processed/time" / f"time_{date_str}.parquet"
    df = pd.DataFrame(datas)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine="fastparquet", index=False)
    print(f"Loaded dim_time into Parquet at {output_path}")

if __name__ == "__main__":
    generate_data_time()