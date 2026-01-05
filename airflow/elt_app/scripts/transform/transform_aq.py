from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_date, hour, lit, udf,arrays_zip
)
from pyspark.sql.types import IntegerType, FloatType,TimestampType
from datetime import datetime
import argparse
from elt_app.utils.config import BUCKET_NAME
from elt_app.utils.logging import get_logger,setup_logging
from elt_app.utils.utils import get_last_file_s3

# ---- BREAKPOINTS ----
pm25_bp = [
    (0, 35, 0, 50),
    (36, 75, 51, 100),
    (76, 115, 101, 150),
    (116, 150, 151, 200),
    (151, 250, 201, 300),
    (251, 350, 301, 400),
    (351, 500, 401, 500)
]

pm10_bp = [
    (0, 50, 0, 50),
    (51, 100, 51, 100),
    (101, 250, 101, 150),
    (251, 350, 151, 200),
    (351, 430, 201, 300),
    (431, 500, 301, 400),
    (501, 600, 401, 500)
]

co_bp = [
    (0, 5, 0, 50),
    (5.1, 10, 51, 100),
    (10.1, 35, 101, 150),
    (35.1, 60, 151, 200),
    (60.1, 90, 201, 300),
    (90.1, 120, 301, 400),
    (120.1, 150, 401, 500)
]

so2_bp = [
    (0, 50, 0, 50),
    (51, 100, 51, 100),
    (101, 350, 101, 150),
    (351, 500, 151, 200),
    (501, 750, 201, 300),
    (751, 1000, 301, 400),
    (1001, 1200, 401, 500)
]

no2_bp = [
    (0, 40, 0, 50),
    (41, 80, 51, 100),
    (81, 180, 101, 150),
    (181, 280, 151, 200),
    (281, 400, 201, 300),
    (401, 500, 301, 400),
    (501, 600, 401, 500)
]

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
            return round((Ihigh - Ilow) / (Chigh - Clow) * (Cp - Clow) + Ilow)
    return None

# ---- DEFINE UDF ----
def calc_aqi(pm25, pm10, co_mg, so2, no2, o3):
    vals = [
        calc_sub_index(pm25, pm25_bp),
        calc_sub_index(pm10, pm10_bp),
        calc_sub_index(co_mg, co_bp),
        calc_sub_index(so2, so2_bp),
        calc_sub_index(no2, no2_bp),
        calc_sub_index(o3, o3_bp)
    ]
    vals = [v for v in vals if v is not None]
    return max(vals) if vals else None

calc_aqi_udf = udf(calc_aqi, IntegerType())


def transform_aq():
    logger = get_logger(__name__, domain_file="aq_spark.log")
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Target date in YYYY-MM-DD format")
    args = parser.parse_args()
    target_date = args.date
    if not target_date:
        raise ValueError("--date is required, format YYYY-MM-DD")
    input_path = get_last_file_s3(f"bronze/fact_aq/event_date={target_date}", ".json")

    if input_path is None:
        logger.error("No AQ JSON found")
        raise ValueError("No AQ JSON found")

    spark = (
        SparkSession.builder
            .appName("Transform AQI")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true")
            .getOrCreate()
        )


    df = spark.read.option("multiline", "true").json(input_path)

    # rename columns
    df = df.withColumnRenamed("pm2_5", "pm25") \
           .withColumnRenamed("carbon_monoxide", "co") \
           .withColumnRenamed("carbon_dioxide", "co2") \
           .withColumnRenamed("nitrogen_dioxide", "no2") \
           .withColumnRenamed("sulphur_dioxide", "so2") \
           .withColumnRenamed("ozone", "o3")

    # explode data
    df = df.withColumn(
        "zipped",
        arrays_zip(
            "time",
            "pm25",
            "pm10",
            "co",
            "no2",
            "so2",
            "o3",
            "co2"
        )
    )

    df = df.withColumn("row", explode(col("zipped"))).select(
        "city_name",
        "city_id",
        "inseget_time",
        col("row.co2").alias("co2"),
        col("row.time").alias("time"),
        col("row.pm25").alias("pm25"),
        col("row.pm10").alias("pm10"),
        col("row.co").alias("co"),
        col("row.no2").alias("no2"),
        col("row.so2").alias("so2"),
        col("row.o3").alias("o3"),
    )

    df = df.withColumn("date", to_date(col("time"))) \
           .withColumn("hour", hour(col("time"))) \
           .withColumn("co_mg", col("co") / lit(1000)) \
           .withColumn("inseget_time", (col("inseget_time") / 1000).cast(TimestampType())) \
           .withColumn("event_date", lit(target_date))

    # calculate AQI
    df = df.withColumn(
        "aqi",
        calc_aqi_udf(
            col("pm25"),
            col("pm10"),
            col("co_mg"),
            col("so2"),
            col("no2"),
            col("o3")
        )
    )
    s3_path = f"s3a://{BUCKET_NAME}/silver/fact_aq/"
    # Ghi DataFrame thành Parquet lên S3
    df.write.mode("overwrite") \
     .partitionBy("event_date") \
     .option("useDeprecatedInt96Timestamps", True) \
     .parquet(s3_path)

    logger.info("Saved AQ Parquet to %s", s3_path)
    spark.stop()


if __name__ == "__main__":
    setup_logging()
    transform_aq()
