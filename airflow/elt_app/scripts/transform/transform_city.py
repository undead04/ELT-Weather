from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit,cast 
from datetime import datetime
from elt_app.utils.logging import get_logger,setup_logging
from elt_app.utils.utils import get_last_file_s3
from elt_app.utils.config import BUCKET_NAME
from pyspark.sql.types import TimestampType,IntegerType,DoubleType

# ==========================================
# SPARK TRANSFORM CITY
# ==========================================
def transform_city():
    logger = get_logger(__name__, domain_file="city_spark.log")
    input_path = get_last_file_s3("bronze/dim_city/", ".json")
    
    if input_path is None:
        logger.error("No bronze city JSON found in S3 → abort transform")
        raise ValueError("Bronze city data not found")

    spark = (
        SparkSession.builder
            .appName("Transform City")
            # Tối ưu cho S3 ghi Parquet
            .config("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true")
            .getOrCreate()
    )

    # 1. Đọc dữ liệu
    df = spark.read.json(input_path)

    # Kiểm tra rỗng hiệu quả hơn df.count()
    if not df.head(1):
        logger.error("Bronze city file is empty → abort transform")
        return 

    # 2. Làm sạch và Transform
    # Lưu ý: "boundingbox" thường là array trong JSON, dùng getItem để lấy index
    df = (df
        .withColumnRenamed("name", "city_name")
        .withColumn("city_id", col("city_id").cast(IntegerType()))
        .withColumn("lat", col("lat").cast(DoubleType()))
        .withColumn("lon", col("lon").cast(DoubleType()))
        # Xử lý epoch milliseconds sang Timestamp
        .withColumn("inseget_time", (col("inseget_time") / 1000).cast(TimestampType()))
        .withColumn("country", lit("Vietnam"))
        # Sửa lỗi lit() sai cách
        .withColumn("min_lat", col("boundingbox").getItem(0).cast(DoubleType()))
        .withColumn("max_lat", col("boundingbox").getItem(1).cast(DoubleType()))
        .withColumn("min_lon", col("boundingbox").getItem(2).cast(DoubleType()))
        .withColumn("max_lon", col("boundingbox").getItem(3).cast(DoubleType()))
    )

    # 3. Loại bỏ rác và trùng lặp
    df = df.dropna(subset=["city_id", "city_name", "lat", "lon"])
    df = df.dropDuplicates(["city_id", "city_name"])

    # 4. Chọn cột cuối cùng
    final_df = df.select(
        "city_id",
        "city_name",
        "lat",
        "lon",
        "country",
        "min_lat",
        "max_lat",
        "min_lon",
        "max_lon",
        "inseget_time"  # Đã sửa lỗi chính tả inseget_time
    )

    out_path = f"s3a://{BUCKET_NAME}/silver/dim_city/dim_city.parquet"

    # 5. Ghi dữ liệu
    # Dùng overwrite để job có thể chạy lại (idempotent)
    final_df.write.mode("overwrite").parquet(out_path)

    logger.info(f"Successfully saved {final_df.count()} rows to {out_path}")
    spark.stop()


if __name__ == "__main__":
    setup_logging()
    transform_city()
