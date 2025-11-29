from pathlib import Path
import pandas as pd
import duckdb
BASE_DIR = Path.cwd()
def get_last_file():
    path = BASE_DIR /  "data" / "processed" / "weather" / "weather_*.parquet"
    files = list(path.parent.glob(path.name))
    if not files:
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return latest_file

def load_weather():

    input_path = get_last_file()
    if input_path is None :
        print("Không tìm thấy bất kỳ file data weather nào")
        return

    database_path = Path.cwd() / "sql/weather.duckdb"
    conn = duckdb.connect(str(database_path))
    conn.execute(f"""
        INSERT INTO fact_weather (
                 city_id,
                 date_id,
                 time_id,
                 temperature,
                 humidity,
                 wind_speed,
                 precipitation,
                 weather_code,
                 cloud_cover,
                 rain,
                 wind_direction,
                 apparent_temperature)
        SELECT 
            city_id,
            d.date_id,
            t.time_id,
            temperature,
            humidity,
            wind_speed,
            precipitation,
            weather_code,
            cloud_cover,
            rain,
            wind_direction,
            apparent_temperature
        FROM '{input_path.as_posix()}' i
        JOIN dim_date d on i.date = d.full_date
        JOIN dim_time t on t.hour = i.hour
            """)
    # Close DuckDB connection
    conn.close()
    print(f"Saved fact_weather to Parquet at {input_path}")
if __name__ == "__main__":
    load_weather()