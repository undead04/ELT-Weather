
from pyspark.sql import SparkSession

def create_spark(app_name="WeatherETL"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")   # bật AQE
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") 
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
        .config("spark.hadoop.fs.s3a.path.style.access", "true") 
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.262")

        .getOrCreate()
    )
    return spark

if __name__ == "__main__":
    spark = create_spark()
    print(spark.version)