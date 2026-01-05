import asyncio
import aiohttp
import pandas as pd
from elt_app.utils.logging import get_logger
import s3fs
from elt_app.utils.config import AWS_BUCKET_ACESS_KEY,AWS_BUCKET_SECRET_KEY,REGION_NAME,BUCKET_NAME
from typing import Optional, Dict, Any
from datetime import datetime,timedelta
from elt_app.utils.logging import setup_logging
from elt_app.utils.utils import get_last_file_s3
import json

logger = get_logger(__name__, domain_file="weather.log")


# ---------- Logging ----------
def log(message: str):
    """Compatibility wrapper — use structured logger instead."""
    logger.info(message)


# ---------- FETCH WITH RETRY ----------
async def fetch_city_weather(
    session: aiohttp.ClientSession,
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    retries: int = 3,
) -> Optional[Dict[str, Any]]:

    url = "https://archive-api.open-meteo.com/v1/archive"
    variables = [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m",
        "wind_speed_10m", "wind_direction_10m", "precipitation",
        "weather_code", "apparent_temperature", "rain", "cloud_cover_low"
    ]

    params = {
        "latitude": str(lat),
        "longitude": str(lon),
        "hourly": ",".join(variables),
        "timezone": "Asia/Singapore",
        "start_date": start_date,
        "end_date": end_date
    }

    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, params=params, timeout=60) as response:

                # ----- Check API limit -----
                if response.status == 429:
                    logger.warning(f"⚠ API limit reached! Retry in 2s (attempt {attempt}/3)")
                    await asyncio.sleep(2)
                    continue

                if response.status != 200:
                    logger.error(f"❌ HTTP {response.status} for lat={lat}, lon={lon}")
                    return None

                data = await response.json()
                hourly = data.get("hourly", {})

                city_data = {var: hourly.get(var, []) for var in variables + ["time"]}
                return city_data

        except Exception as e:
            logger.warning(f"⚠ Error attempt {attempt}/3 for {lat},{lon} → {e}")
            await asyncio.sleep(1)

    logger.error(f"❌ Failed after retries: {lat},{lon}")
    return None


# ---------- CRAWL ALL ----------
async def crawl_all_cities(cities: pd.DataFrame, start_date: str, end_date: str):
    all_data = []

    connector = aiohttp.TCPConnector(limit=10)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_city_weather(session, row.lat, row.lon, start_date, end_date)
            for row in cities.itertuples()
        ]

        results = await asyncio.gather(*tasks)
        inseget_time = datetime.now()
        for row, data in zip(cities.itertuples(), results):
            if data:
                data['city_id'] = row.city_id
                data["city_name"] = row.city_name
                data["inseget_time"] = inseget_time
                all_data.append(data)
            else:
                log(f"❌ Failed: {row.city_name}")

    return all_data


# ---------- EXTRACT MAIN ----------
def extract_weather(**context):
    target_date = context['ds']
    logger.info(f"===== START WEATHER CRAWL {target_date} =====")

    cities_path = get_last_file_s3("silver/dim_city/", ".parquet")
    if not cities_path:
        logger.error("❌ No city parquet found.")
        raise ValueError("No city parquet found")

    cities = pd.read_parquet(cities_path)
    logger.info(f"Loaded {len(cities)} cities from {cities_path}")


    logger.info(f"Crawling range: {target_date}")

    start_time = datetime.now()
    start_date = target_date
    end_date = target_date
    
    all_data = asyncio.run(crawl_all_cities(cities, start_date,end_date))

    df = pd.DataFrame(all_data)
    df.info()
    df.head()
    df.describe()
    if df.empty:
        logger.info("No data to save")
        raise ValueError("No data to save")
    
    prefix = "bronze/fact_weather/"
    
    bucket = BUCKET_NAME

    fs = s3fs.S3FileSystem(
        key=AWS_BUCKET_ACESS_KEY,
        secret=AWS_BUCKET_SECRET_KEY,
        client_kwargs={'region_name': REGION_NAME}
    )

    s3_path = f"s3://{bucket}/{prefix}event_date={target_date}/fact_weather.json"
    # Sử dụng context manager 'with' để đảm bảo đóng stream sau khi ghi
    with fs.open(s3_path, 'w') as f:
        df.to_json(
            f, 
            orient="records", 
            lines=True,
            force_ascii=False # Thêm cái này nếu dữ liệu có tiếng Việt
        )

    duration = (datetime.now() - start_time).seconds
    logger.info(f"✔ Saved: S3: s3://%s/%s", BUCKET_NAME, s3_path)
    logger.info(f"⏱ Total time: {duration} seconds")
    logger.info(f"===== FINISHED WEATHER CRAWL {target_date} =====\n")


if __name__ == "__main__":
    setup_logging()
    extract_weather()
