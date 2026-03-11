import asyncio
import aiohttp
import pandas as pd
from etl_app.utils import get_logger, setup_logging, DB_URL
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path
from sqlalchemy import create_engine

logger = get_logger(__name__, domain_file="aq.log")


# ---------- Logging ----------
def log(message: str):
    logger.info(message)


async def fetch_city_aq(
    session: aiohttp.ClientSession,
    lats: List[float],
    lons: List[float],
    retries: int = 3,
) -> Optional[List[Dict[str, Any]]]:
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    variables = [
        "pm2_5",
        "european_aqi_pm2_5",
    ]
    params = {
        "latitude": ",".join(map(str, lats)),
        "longitude": ",".join(map(str, lons)),
        "current": ",".join(variables),
        "timezone": "GMT",
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
                    current = entry.get("current", {})
                    city_data = {var: current.get(var) for var in variables + ["time"]}
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
            tasks.append(fetch_city_aq(session, lats, lons))

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


def extract_aq_current():
    target_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"===== START AQ CURRENT CRAWL {target_date} =====")

    cities_file = (
        Path(__file__).resolve().parent.parent.parent / "data/dim_locations.csv"
    )
    if not cities_file.exists():
        raise FileNotFoundError(f"No city location found at {cities_file}")

    cities = pd.read_csv(cities_file)
    total_cities = len(cities)
    logger.info(f"📍 Loaded {total_cities} cities from {cities_file}")

    logger.info(f"Crawling {target_date}")
    start_time = datetime.now()

    all_data = asyncio.run(crawl_all_cities(cities))

    df = pd.DataFrame(all_data)
    if df.empty:
        logger.info("No data to save")
        raise ValueError("No data crawled for current air quality")

    records_crawled = len(all_data)
    failed_records = total_cities - records_crawled

    logger.info(f"📊 CRAWL SUMMARY:")
    logger.info(f"  ├─ Total cities: {total_cities}")
    logger.info(f"  ├─ ✅ Successfully crawled: {records_crawled} records")
    logger.info(f"  └─ ❌ Failed: {failed_records} records")

    schema = "raw"
    table = "raw_aq_current"
    engine = create_engine(DB_URL)
    df.to_sql(table, engine, schema=schema, if_exists="append", index=False)

    duration = (datetime.now() - start_time).seconds
    logger.info(f"💾 Inserted {len(df)} records into {schema}.{table}")
    logger.info(f"✔ Saved current: {schema}.{table}")
    logger.info(f"⏱ Total time: {duration} seconds")
    logger.info(f"===== FINISHED AQ CURRENT CRAWL {target_date} =====\n")

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
    extract_aq_current()
