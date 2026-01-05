import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from elt_app.scripts.transform.transform_weather import process_weather_data

def test_weather_transformation_logic(spark):
    """
    Test the process_weather_data function.
    We mock the INPUT schema which should mimic the raw JSON structure
    (arrays of data) before it gets exploded.
    """
    # 1. Mock Input Data (Structure similar to what spark.read.json returns)
    # The script renames 'temperature_2m' -> 'temperature', etc.
    # So we must provide the ORIGINAL names.
    
    schema = StructType([
        StructField("city_name", StringType(), True),
        StructField("time", ArrayType(StringType()), True),
        StructField("temperature_2m", ArrayType(DoubleType()), True),
        StructField("relative_humidity_2m", ArrayType(DoubleType()), True),
        StructField("wind_speed_10m", ArrayType(DoubleType()), True),
        StructField("weather_code", ArrayType(LongType()), True),
        StructField("precipitation", ArrayType(DoubleType()), True),
        StructField("cloud_cover_low", ArrayType(DoubleType()), True),
        StructField("rain", ArrayType(DoubleType()), True),
        StructField("wind_direction_10m", ArrayType(DoubleType()), True),
        StructField("apparent_temperature", ArrayType(DoubleType()), True),
        # Columns not used in select but might be present? 
        # The script renames specific ones. We only provide used ones.
         StructField("dew_point_2m", ArrayType(DoubleType()), True)
    ])

    data = [
        (
            "Hanoi",
            ["2025-01-01T12:00"],       # time
            [30.5],                     # temperature
            [70.0],                     # humidity
            [10.0],                     # wind_speed
            [1],                        # weather_code (1 = Chủ yếu quang đãng)
            [0.0],                      # precipitation
            [10.0],                     # cloud_cover
            [0.0],                      # rain
            [180.0],                    # wind_direction
            [32.0],                     # apparent_temperature
            [25.0]                      # dew_point
        )
    ]

    df_input = spark.createDataFrame(data, schema)

    # 2. Run Transformation
    df_output = process_weather_data(df_input)

    # 3. Assertions
    results = df_output.collect()
    assert len(results) == 1
    row = results[0]

    # Verify column existence and values
    assert row.city_name == "Hanoi"
    assert row.temperature == 30.5
    assert row.weather_code == 1        # Should be int
    assert row.weather_type == "Chủ yếu quang đãng" # Check if mapping UDF worked
    assert row.hour == 12
    assert str(row.date) == "2025-01-01"

    print("Test passed!")
