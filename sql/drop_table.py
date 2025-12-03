import duckdb
from pathlib import Path
from utils.config import SQL_DB_PATH
database_path = SQL_DB_PATH

# Kết nối đến DuckDB, tạo hoặc mở cơ sở dữ liệu
conn = duckdb.connect(database=database_path)
conn.execute("""
DROP TABLE IF EXISTS  fact_air_quality
""")

# Xóa table
conn.execute("""CREATE TABLE fact_air_quality (
    aq_id INTEGER PRIMARY KEY DEFAULT nextval('aq_id_seq'),
    time_id INTEGER NOT NULL,
    city_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    aqi DOUBLE NOT NULL,
    pm25 DOUBLE NOT NULL,
    pm10 DOUBLE NOT NULL,
    no2 DOUBLE NOT NULL,
    so2 DOUBLE NOT NULL,
    o3 DOUBLE NOT NULL,
    co DOUBLE NOT NULL,
    co2 DOUBLE NOT NULL,
    FOREIGN KEY (city_id) REFERENCES dim_city(city_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
)""")
# Đóng kết nối
conn.close()