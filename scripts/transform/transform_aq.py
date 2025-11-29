# elt_scripts/load/load_duckdb.py
from importlib.metadata import files
from datetime import datetime
import pandas as pd 
from pathlib import Path
BASE_DIR = Path.cwd()
# breakpoint cho PM2.5 (µg/m³)
pm25_bp = [
    (0, 35, 0, 50),
    (36, 75, 51, 100),
    (76, 115, 101, 150),
    (116, 150, 151, 200),
    (151, 250, 201, 300),
    (251, 350, 301, 400),
    (351, 500, 401, 500)
]

# breakpoint cho PM10 (µg/m³)
pm10_bp = [
    (0, 50, 0, 50),
    (51, 100, 51, 100),
    (101, 250, 101, 150),
    (251, 350, 151, 200),
    (351, 430, 201, 300),
    (431, 500, 301, 400),
    (501, 600, 401, 500)
]

# breakpoint cho CO (mg/m³)
co_bp = [
    (0, 5, 0, 50),
    (5.1, 10, 51, 100),
    (10.1, 35, 101, 150),
    (35.1, 60, 151, 200),
    (60.1, 90, 201, 300),
    (90.1, 120, 301, 400),
    (120.1, 150, 401, 500)
]

# breakpoint cho SO2 (µg/m³)
so2_bp = [
    (0, 50, 0, 50),
    (51, 100, 51, 100),
    (101, 350, 101, 150),
    (351, 500, 151, 200),
    (501, 750, 201, 300),
    (751, 1000, 301, 400),
    (1001, 1200, 401, 500)
]

# breakpoint cho NO2 (µg/m³)
no2_bp = [
    (0, 40, 0, 50),
    (41, 80, 51, 100),
    (81, 180, 101, 150),
    (181, 280, 151, 200),
    (281, 400, 201, 300),
    (401, 500, 301, 400),
    (501, 600, 401, 500)
]
# breakpoint cho o3 (µg/m³)
o3_bp = [
    (0, 100, 0, 50),
    (101, 160, 51, 100),
    (161, 215, 101, 150),
    (216, 265, 151, 200),
    (266, 800, 201, 300),
    (801, 1000, 301, 400),
    (1001, 1200, 401, 500)
]
def calc_sub_index(Cp, breakpoints):
    for (Clow, Chigh, Ilow, Ihigh) in breakpoints:
        if Clow <= Cp <= Chigh:
            I = (Ihigh - Ilow) / (Chigh - Clow) * (Cp - Clow) + Ilow
            return round(I)
    return None  # ngoài phạm vi
def calc_aqi(pm25, pm10, co, so2, no2,o3):
    sub_indices = [
        calc_sub_index(pm25, pm25_bp),
        calc_sub_index(pm10, pm10_bp),
        calc_sub_index(co, co_bp),
        calc_sub_index(so2, so2_bp),
        calc_sub_index(no2, no2_bp),
        calc_sub_index(o3,o3_bp)
    ]
    # AQI = max(sub-index)
    return max(filter(lambda x: x is not None, sub_indices))

def get_last_file():
    path = BASE_DIR /  "data" / "raw" / "aq" / "aq_*.json"
    files = list(path.parent.glob(path.name))
    if not files:
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return latest_file

def transform_aq():
    folder_path = get_last_file()
    if folder_path is None:
        print("No aq data folder found.")
        return
    
    cols = ["co","co2","no2","so2","o3","pm10","pm25"]
    input_path = get_last_file()
    df = pd.read_json(input_path)
    df.rename(columns={
            "pm2_5":"pm25",
            "carbon_monoxide":"co",
            "carbon_dioxide":"co2",
            "nitrogen_dioxide":"no2",
            "sulphur_dioxide":"so2",
            "ozone":"o3",
        }, inplace=True)
    df.drop_duplicates(subset=(["city_id"]))
    df_exploded = df.explode(cols+['time']).reset_index(drop=True)
    df_exploded.fillna({col: 0 for col in cols}, inplace=True)
    df_exploded["date"] = pd.to_datetime(df_exploded["time"]).dt.date
    df_exploded["hour"] = pd.to_datetime(df_exploded["time"]).dt.hour
    df_exploded['co_mg'] = df_exploded['co'] / 1000  # µg/m³ → mg/m³
    df_exploded['aqi'] = df_exploded.apply(
        lambda row: calc_aqi(row['pm25'],row['pm10'], 
                             row['co_mg'], row['so2'], row['no2'],row['o3']),
        axis=1
    )
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_parquet_path = BASE_DIR / f"data/processed/aq/aq_{date_str}.parquet"
    out_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df_exploded.to_parquet(out_parquet_path,engine="pyarrow", index=False)
    print(f"Saved aq parquet to {out_parquet_path}")
if __name__ == '__main__':
    transform_aq()
