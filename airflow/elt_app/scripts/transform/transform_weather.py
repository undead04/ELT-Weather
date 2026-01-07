from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_date, hour, lit, arrays_zip,to_timestamp,coalesce,create_map
)
from itertools import chain
import argparse
from pyspark.sql.types import IntegerType, FloatType,TimestampType
from elt_app.utils.logging import get_logger,setup_logging
from elt_app.utils.utils import get_last_file_s3
from elt_app.utils.config import BUCKET_NAME
WEATHER_CODE_VI = {
    0: "Trời quang đãng",

    1: "Chủ yếu quang đãng",
    2: "Có mây rải rác",
    3: "Nhiều mây / u ám",

    45: "Sương mù",
    48: "Sương mù đóng băng",

    51: "Mưa phùn nhẹ",
    53: "Mưa phùn vừa",
    55: "Mưa phùn dày",

    56: "Mưa phùn đóng băng nhẹ",
    57: "Mưa phùn đóng băng dày",

    61: "Mưa nhẹ",
    63: "Mưa vừa",
    65: "Mưa to",

    66: "Mưa đóng băng nhẹ",
    67: "Mưa đóng băng mạnh",

    71: "Tuyết rơi nhẹ",
    73: "Tuyết rơi vừa",
    75: "Tuyết rơi dày",

    77: "Hạt tuyết",

    80: "Mưa rào nhẹ",
    81: "Mưa rào vừa",
    82: "Mưa rào dữ dội",

    85: "Mưa tuyết nhẹ",
    86: "Mưa tuyết mạnh",

    95: "Dông / giông bão",
    96: "Dông kèm mưa đá nhẹ",
    99: "Dông kèm mưa đá lớn"
}

def transform_weather():
    logger = get_logger(__name__, domain_file="weather.log")
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Target date in YYYY-MM-DD format")
    args = parser.parse_args()
    target_date = args.date
    if not target_date:
        raise ValueError("--date is required, format YYYY-MM-DD")

    input_path = get_last_file_s3(f"bronze/fact_weather/event_date={target_date}", ".json")
    if input_path is None:
        logger.error(f"No weather JSON found. Target date: {target_date}")
        raise ValueError(f"No weather JSON found. Target date: {target_date}")

    # ==== 2) Init Spark ====
    spark = (
        SparkSession.builder
        .appName("Transform Weather")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true")
        .getOrCreate()
    )

    # ==== 3) Load raw JSON ====
    df = spark.read.option("multiline", "true").json(input_path)
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
        "city_id",
        "inseget_time",
        col("row.time").alias("time"),
        col("row.temperature").alias("temperature"),
        col("row.humidity").alias("humidity"),
        col("row.wind_speed").alias("wind_speed"),
        col("row.weather_code").cast("int").alias("weather_code"),
        col("row.precipitation").alias("precipitation"),
        col("row.cloud_cover").alias("cloud_cover"),
        col("row.rain").alias("rain"),
        col("row.wind_direction").alias("wind_direction"),
        col("row.apparent_temperature").alias("apparent_temperature"),
    )

    # ==== 6) Add date, hour ====# 3. Mapping Weather Code (Không dùng Lambda/UDF)
    mapping_expr = create_map([lit(x) for x in chain(*WEATHER_CODE_VI.items())])
    
    df = (df
        .withColumn("time", to_timestamp("time"))
        .withColumn("date", to_date("time"))
        .withColumn("hour", hour("time"))
        .withColumn("weather_code", col("weather_code").cast("int"))
        .withColumn("weather_type", coalesce(mapping_expr.getItem(col("weather_code")), lit("Khác")))
        .withColumn("event_date", lit(target_date.replace("-", "")))
        # Xử lý inseget_time an toàn
        .withColumn("inseget_time", (col("inseget_time") / 1000).cast(TimestampType())) 
    )
    # ==== 7) Ghi lên S3 STAGING ====
    s3_path = f"s3a://{BUCKET_NAME}/silver/fact_weather/"

    df.write.mode("overwrite") \
     .partitionBy("event_date") \
     .option("useDeprecatedInt96Timestamps", True) \
     .parquet(s3_path)

    logger.info(f"Uploaded Spark-parquet to S3: {s3_path}")

    spark.stop()


if __name__ == "__main__":
    setup_logging()
    transform_weather()
