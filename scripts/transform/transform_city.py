import pandas as pd
from pathlib import Path
from datetime import datetime
def get_last_file():
    path = Path.cwd() /  "data" / "raw" / "city" / "city_*.json"
    files = list(path.parent.glob(path.name))
    if not files:
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return latest_file

def transform_city():
    input_path = get_last_file()
    if input_path is None:
        print("No city data file found.")
        return
    
    df = pd.read_json(input_path, encoding='utf-8')
    df.dropna(axis=0,how="any",subset=["city_id", "name", "lat", "lon"],inplace=True)
    df.drop_duplicates(subset=["city_id","name","lat","lon"],inplace=True)
    df = df.rename(columns={
        "name": "city_name",
    })
    df_cleared = df[["city_id", "city_name", "lat", "lon"]].copy()
    df_finished = df_cleared
    df_finished["country"] = "Việt Nam"  # tất cả dòng đều là 'Vietnam'
    # Lưu file đã chuyển đổi
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_path = Path.cwd() / "data" / "processed" / "city" / f"city_{date_str}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_finished.to_parquet(output_path, index=False)
    BASE_DIR = Path.cwd()
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_path = BASE_DIR / f"data/processed/city/city_{date_str}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # ghi DataFrame thành file Parquet
    df_finished.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"Loaded dim_city from Parquet at {output_path}")

if __name__ == "__main__":
    transform_city()