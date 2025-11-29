import duckdb
from pathlib import Path
# Đường dẫn tới file DuckDB
database_path = Path.cwd() / "sql" / "weather.duckdb"

# Xóa file cơ sở dữ liệu nếu tồn tại
if database_path.exists():
    database_path.unlink()
# Kết nối đến DuckDB, tạo hoặc mở cơ sở dữ liệu
conn = duckdb.connect(database=database_path)

# Đọc nội dung của file SQL
with open(Path.cwd() / "sql" / "Datawarehouse.sql", 'r') as file:
    sql_script = file.read()

# Chạy các câu lệnh SQL từ file
conn.execute(sql_script)

# Đóng kết nối
conn.close()

print(f"Database đã được tạo và lưu vào {database_path}")
