# ELT Weather Project

Dự án Data Engineering xây dựng quy trình ELT (Extract-Load-Transform) để thu thập, xử lý và phân tích dữ liệu Thời tiết (Weather) và Chất lượng không khí (Air Quality) cho các thành phố tại Việt Nam.

Hệ thống được thiết kế để chạy hoàn toàn trên Docker, sử dụng các công nghệ phổ biến trong ngành dữ liệu.

## 🏗 Kiến trúc & Công nghệ

Dự án sử dụng các công nghệ sau:

- **Orchestration**: [Apache Airflow](https://airflow.apache.org/) - Lên lịch và quản lý workflow (DAGs).
- **Data Processing**: [Apache Spark](https://spark.apache.org/) (PySpark) - Xử lý dữ liệu phân tán, clean và transform dữ liệu.
- **Data Warehouse**: [PostgreSQL](https://www.postgresql.org/) - Lưu trữ dữ liệu đã qua xử lý (Warehouse).
- **Infrastructure**: Docker & Docker Compose - Quản lý môi trường và service.
- **Storage**: S3 (MinIO hoặc AWS S3) - Data Lake (Raw data).

## 📂 Cấu trúc dự án

```
ELT Weather/
├── airflow/
│   ├── dags/               # Chứa các Airflow DAGs (etl_day, etl_year)
│   ├── elt_app/            # Source code chính của ứng dụng
│   │   ├── scripts/        # Python scripts cho các bước (extract, transform, load)
│   │   ├── sql/            # Các câu lệnh SQL (Merge, DDL)
│   │   └── utils/          # Các hàm tiện ích (Config, Logging...)
│   ├── Dockerfile.airflow  # Custom image cho Airflow
│   └── requirements.txt    # Thư viện Python cho Airflow
├── spark/
│   ├── conf/               # Cấu hình Spark (spark-defaults.conf)
│   ├── jars/               # Các thư viện Java/Scala cần thiết (JDBC, AWS...)
│   └── Dockerfile.spark    # Custom image cho Spark
├── postgres-init/          # Script khởi tạo database PostgreSQL
├── docker-compose.yml      # File cấu hình toàn bộ stack
└── README.md               # Tài liệu dự án
```

## 🚀 Cài đặt và Chạy dự án

### Tiền đề (Prerequisites)
- [Docker](https://www.docker.com/) và [Docker Compose](https://docs.docker.com/compose/) đã được cài đặt trên máy.
- Git.

### Các bước triển khai

1. **Clone repository:**
   ```bash
   git clone <repo_url>
   cd "ELT Weather"
   ```

2. **Cấu hình biến môi trường:**
   Tạo file `.env` (nếu chưa có) và cập nhật các thông tin cần thiết:
   ```env
   # AWS / S3 Config
   AWS_ACCESS_KEY_ID=minioadmin
   AWS_SECRET_ACCESS_KEY=minioadmin
   AWS_DEFAULT_REGION=us-east-1
   BUCKET=weather-data

   # Cấu hình Airflow & Database
   AIRFLOW_UID=50000
   POSTGRES_USER=airflow
   POSTGRES_PASSWORD=airflow
   POSTGRES_DB=warehouse
   ```

3. **Khởi chạy hệ thống với Docker Compose:**
   ```bash
   docker compose up -d --build
   ```
   Lệnh này sẽ build các images (Airflow, Spark) và khởi động các containers:
   - `postgres`: Database cho Airflow metadata và Data Warehouse.
   - `airflow-webserver`: Giao diện quản lý Airflow.
   - `airflow-scheduler`: Bộ lập lịch của Airflow.
   - `spark-master` & `spark-worker`: Cluster Spark.

4. **Truy cập giao diện quản trị:**
   - **Airflow UI**: http://localhost:8080 (Tài khoản mặc định: `admin`/`admin` - xem trong `docker-compose.yml` phần `airflow-init`).
   - **Spark Master UI**: http://localhost:8081.

## 🏃‍♂️ Sử dụng Pipeline

Dự án bao gồm các DAGs chính trong Airflow:

1. **`etl_daily`**: Chạy hàng ngày để lấy dữ liệu thời tiết và AQI hiện tại.
2. **`etl_yearly`**: (Tùy chọn) Chạy định kỳ để lấy dữ liệu lịch sử hoặc tổng hợp theo năm.

### Luồng xử lý dữ liệu (Workflow):

1. **Extract**: Gọi API thời tiết/AQI, lưu dữ liệu thô (JSON) vào S3 (Data Lake).
2. **Transform**: Spark đọc dữ liệu từ S3, làm sạch, chuyển đổi cấu trúc và lưu lại dưới dạng Parquet.
3. **Load**: Load dữ liệu từ Parquet vào PostgreSQL (Data Warehouse) sử dụng cơ chế Merge (Upsert) để tránh trùng lặp.

## 🛠 Phát triển (Development)

Để chạy thử nghiệm các script Python cục bộ (không qua Docker), bạn cần thiết lập môi trường ảo:

```bash
# Tạo môi trường ảo
python -m venv .venv
source .venv/bin/activate  # Hoặc config .venv\Scripts\activate trên Windows

# Cài đặt thư viện
pip install -r airflow/requirements.txt
```

### Chạy thủ công (Ví dụ)
Nếu muốn chạy một file script cụ thể:
```bash
python airflow/elt_app/scripts/extract/weather.py
```
*Lưu ý: Cần đảm bảo các biến môi trường được set đúng trong phiên làm việc local.*

## 📝 Liên hệ

Dự án được thực hiện bởi Trần Văn An. Mọi thắc mắc vui lòng liên hệ qua antran.261004@gmail.com hoặc tạo Issue trên repo này.
