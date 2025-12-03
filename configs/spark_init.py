from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from utils.config import SPARK_APP_NAME, DELTA_DIR


def create_spark(app_name: str = None, warehouse_dir: str = None) -> SparkSession:
    """Create a SparkSession configured with Delta Lake support.

    This uses delta-spark helper configure_spark_with_delta_pip so the same
    environment can work with pip-installed delta-spark.
    """
    app_name = app_name or SPARK_APP_NAME
    warehouse_dir = warehouse_dir or str(DELTA_DIR)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_dir)
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark
