import duckdb
from pathlib import Path
from utils.config import SQL_SCRIPTS_PATH,SQL_DB_PATH
from utils.logging import get_logger
logger = get_logger(__name__, domain_file="sql_setup.log")
scripts = SQL_SCRIPTS_PATH
database_path = SQL_DB_PATH
# Xóa file cơ sở dữ liệu nếu tồn tại
if database_path.exists():
    database_path.unlink()
    
# Kết nối đến DuckDB, tạo hoặc mở cơ sở dữ liệu
conn = duckdb.connect(database=database_path)

# Đọc nội dung của file SQL
with open(scripts / "Datawarehouse.sql", 'r') as file:
    sql_script = file.read()

# Chạy các câu lệnh SQL từ file
conn.execute(sql_script)

# Đóng kết nối
conn.close()

logger.info("Database đã được tạo và lưu vào %s", database_path)
