from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit,cast 
from datetime import datetime
from elt_app.utils.logging import get_logger,setup_logging
from elt_app.utils.utils import get_last_file_s3
from elt_app.utils.config import BUCKET_NAME

# ==========================================
# SPARK TRANSFORM CITY
# ==========================================
def transform_city():

    logger = get_logger(__name__, domain_file="city_spark.log")

    input_path = get_last_file_s3("raw/city/",".json")
    
    if input_path is None:
        logger.error("No city JSON found in S3")
        return

    # SparkSession có cấu hình đọc/ghi S3
    spark = (
        SparkSession.builder
            .appName("Transform City")
            .getOrCreate()
    )

    # đọc file JSON
    df = spark.read.option("multiLine", True).json(input_path)
    df.printSchema()
    df.show(5)

    # loại bỏ null
    df = df.dropna(subset=["city_id", "name", "lat", "lon"])

    # rename cột
    df = df.withColumnRenamed("name", "city_name")

    # chọn cột chuẩn
    df = df.select(
        "city_id",
        "city_name",
        "lat",
        "lon"
    ).dropDuplicates(["city_id", "city_name"])
    # Giả sử df có cột "lat" và "lon" đang là String
    df = df.withColumn("lat", col("lat").cast("double")) \
        .withColumn("lon", col("lon").cast("double")) \
        .withColumn("city_id", col("city_id").cast("int"))
    # thêm cột country
    df = df.withColumn("country", lit("Vietnam"))

    # Output path trên S3
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_path = f"s3a://{BUCKET_NAME}/staging/city/city_{date_str}.parquet"

    # ghi parquet lên S3
    df.write.mode("overwrite").option("useDeprecatedInt96Timestamps", True).parquet(out_path)

    logger.info(f"Saved city parquet to {out_path}")

    spark.stop()


if __name__ == "__main__":
    setup_logging()
    transform_city()
