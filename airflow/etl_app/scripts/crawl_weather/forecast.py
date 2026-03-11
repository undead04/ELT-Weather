import asyncio
import aiohttp
import pandas as pd
from etl_app.utils import get_logger, setup_logging, DB_URL
from typing import List, Optional, Dict, Any
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine

logger = get_logger(__name__, domain_file="weather.log")


# ---------- Logging ----------
def log(message: str):
    logger.info(message)


async def fetch_city_weather(
    session: aiohttp.ClientSession,
    lats: List[float],
    lons: List[float],
    retries: int = 3,
) -> Optional[List[Dict[str, Any]]]:
    url = "https://api.open-meteo.com/v1/forecast"
    variables = [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "uv_index",
        "precipitation",
        "wind_speed_10m",
    ]
    params = {
        "latitude": ",".join(map(str, lats)),
        "longitude": ",".join(map(str, lons)),
        "hourly": ",".join(variables),
        "timezone": "GMT",
        "forecast_days": 1,
    }
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, params=params, timeout=60) as response:
                if response.status == 429:
                    logger.warning(
                        f"⚠ API limit reached! Retry in 2s (attempt {attempt}/3)"
                    )
                    await asyncio.sleep(2)
                    continue

                if response.status != 200:
                    logger.error(f"❌ HTTP {response.status} for {len(lats)} locations")
                    return None

                data = await response.json()
                if not isinstance(data, list):
                    data = [data]

                results = []
                for entry in data:
                    hourly = entry.get("hourly", {})
                    # Map wind_speed_10m to wind_speed for consistent naming with staging
                    city_data = {
                        "temperature_2m": hourly.get("temperature_2m", []),
                        "relative_humidity_2m": hourly.get("relative_humidity_2m", []),
                        "apparent_temperature": hourly.get("apparent_temperature", []),
                        "uv_index": hourly.get("uv_index", []),
                        "precipitation": hourly.get("precipitation", []),
                        "wind_speed": hourly.get("wind_speed_10m", []),
                        "time": hourly.get("time", []),
                    }
                    results.append(city_data)
                return results
        except Exception as e:
            logger.warning(f"⚠ Error attempt {attempt}/3 for batch → {e}")
            await asyncio.sleep(1)

    logger.error(f"❌ Failed after retries for batch")
    return None


async def crawl_all_cities(cities: pd.DataFrame):
    all_data = []
    batch_size = 30
    connector = aiohttp.TCPConnector(limit=10)

    city_chunks = [
        cities[i : i + batch_size] for i in range(0, len(cities), batch_size)
    ]

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for chunk in city_chunks:
            lats = chunk.latitude.tolist()
            lons = chunk.longitude.tolist()
            tasks.append(fetch_city_weather(session, lats, lons))

        batch_results = await asyncio.gather(*tasks)

        insert_time = datetime.now()
        for chunk, results in zip(city_chunks, batch_results):
            if results:
                for row, data in zip(chunk.itertuples(), results):
                    if data:
                        data["province_id"] = row.province_id
                        data["insert_time"] = insert_time
                        all_data.append(data)
            else:
                logger.error(
                    f"❌ Failed batch for provinces: {chunk.province_id.tolist()}"
                )
    return all_data


def extract_weather_forecast():
    target_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"===== START WEATHER FORECAST CRAWL {target_date} =====")

    # Resolve path relative to this script
    base_dir = Path(__file__).resolve().parent.parent.parent

    province_path = base_dir / "data/dim_locations.csv"
    if not province_path.exists():
        province_path_parquet = base_dir / "data/dim_province.parquet"
        if province_path_parquet.exists():
            province_path = province_path_parquet
            cities = pd.read_parquet(province_path)
        else:
            logger.error("❌ No province file found.")
            raise FileNotFoundError(f"No province file found at {base_dir / 'data'}")
    else:
        cities = pd.read_csv(province_path)

    total_cities = len(cities)
    logger.info(f"📍 Loaded {total_cities} cities from {province_path}")
    logger.info(f"Crawling range: {target_date}")

    start_time = datetime.now()

    all_data = asyncio.run(crawl_all_cities(cities))

    df = pd.DataFrame(all_data)
    if df.empty:
        logger.info("No data to save")
        raise ValueError("No data crawled for weather forecast")

    records_crawled = len(all_data)
    failed_records = total_cities - records_crawled

    logger.info(f"📊 CRAWL SUMMARY:")
    logger.info(f"  ├─ Total cities: {total_cities}")
    logger.info(f"  ├─ ✅ Successfully crawled: {records_crawled} records")
    logger.info(f"  └─ ❌ Failed: {failed_records} records")

    schema = "raw"
    table = "raw_weather_forecast"
    engine = create_engine(DB_URL)
    df.to_sql(table, engine, schema=schema, if_exists="append", index=False)

    duration = (datetime.now() - start_time).seconds
    logger.info(f"💾 Inserted {len(df)} records into {schema}.{table}")
    logger.info(f"✔ Saved forecast: {schema}.{table}")
    logger.info(f"⏱ Total time: {duration} seconds")
    logger.info(f"===== FINISHED WEATHER FORECAST CRAWL {target_date} =====\n")

    # Return metadata for Airflow XCom
    return {
        "total_cities": total_cities,
        "records_crawled": records_crawled,
        "records_inserted": len(df),
        "table": f"{schema}.{table}",
        "duration_seconds": duration,
        "failed_records": failed_records,
    }


if __name__ == "__main__":
    setup_logging()
    extract_weather_forecast()
