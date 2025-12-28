from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_date, hour, lit, arrays_zip
)
from elt_app.utils.logging import get_logger,setup_logging
from elt_app.utils.utils import get_last_file_s3
from elt_app.utils.config import BUCKET_NAME

def transform_weather():
    logger = get_logger(__name__, domain_file="weather.log")

    # ==== 1) Tìm file mới nhất ====
    input_path = get_last_file_s3("raw/weather/", ".json")
    if input_path is None:
        logger.error("No weather JSON found.")
        return

    # ==== 2) Init Spark ====
    spark = (
        SparkSession.builder
        .appName("Transform Weather")
        .getOrCreate()
    )

    # ==== 3) Load raw JSON ====
    df = spark.read.option("multiLine", True).json(input_path)
    df.printSchema()
    # ==== 4) Rename columns giống Pandas version ====
    df = df.withColumnRenamed("temperature_2m", "temperature") \
           .withColumnRenamed("relative_humidity_2m", "humidity") \
           .withColumnRenamed("dew_point_2m", "dew_point") \
           .withColumnRenamed("wind_speed_10m", "wind_speed") \
           .withColumnRenamed("wind_direction_10m", "wind_direction") \
           .withColumnRenamed("weather_code", "weather_code") \
           .withColumnRenamed("cloud_cover_low", "cloud_cover")

    df = df.withColumn(
        "zipped",
        arrays_zip(
            "time",
            "temperature",
            "humidity",
            "wind_speed",
            "weather_code",
            "precipitation",
            "cloud_cover",
            "rain",
            "wind_direction",
            "apparent_temperature"
        )
    )

    df = df.withColumn("row", explode(col("zipped"))).select(
        "city_name",
        col("row.time").alias("time"),
        col("row.temperature").alias("temperature"),
        col("row.humidity").alias("humidity"),
        col("row.wind_speed").alias("wind_speed"),
        col("row.weather_code").alias("weather_code"),
        col("row.precipitation").alias("precipitation"),
        col("row.cloud_cover").alias("cloud_cover"),
        col("row.rain").alias("rain"),
        col("row.wind_direction").alias("wind_direction"),
        col("row.apparent_temperature").alias("apparent_temperature"),
    )

    # ==== 6) Add date, hour ====
    df = df.withColumn("date", to_date(col("time"))) \
           .withColumn("hour", hour(col("time")))

    # ==== 7) Ghi lên S3 STAGING ====
    date_str = datetime.now().strftime("%Y-%m-%d")
    s3_path = f"s3a://{BUCKET_NAME}/staging/weather/weather_{date_str}.parquet"

    df.write.mode("overwrite").option("useDeprecatedInt96Timestamps", True).parquet(s3_path)

    logger.info(f"Uploaded Spark-parquet to S3: {s3_path}")

    spark.stop()


if __name__ == "__main__":
    setup_logging()
    transform_weather()
