import asyncio
import aiohttp
import pandas as pd
from elt_app.utils.config import  BUCKET_NAME, REGION_NAME,AWS_BUCKET_ACESS_KEY,AWS_BUCKET_SECRET_KEY
from elt_app.utils.logging import get_logger, setup_logging
import json
import boto3
from datetime import datetime,timedelta
from typing import Optional, Dict, Any
from elt_app.utils.utils import get_last_file_s3

logger = get_logger(__name__, domain_file="aq.log")

# ---------- Logging ----------
def log(message: str):
    logger.info(message)

async def fetch_city_aq(
        session:aiohttp.ClientSession,
        lat:float, lon:float, 
        start_date:str, 
        end_date:str,
        retries:int = 3) -> Optional[Dict[str, Any]]:
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    variables = ["pm10", "pm2_5", "carbon_monoxide", 
                 "carbon_dioxide", "nitrogen_dioxide", 
                 "sulphur_dioxide", "ozone"]  
    params = {
        "latitude": lat,
        "longitude": lon,
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

async def crawl_all_cities(cities:pd.DataFrame, start_date:str, end_date:str):
    all_data = []

    connector = aiohttp.TCPConnector(limit=10)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_city_aq(session, row.lat, row.lon, start_date, end_date)
            for row in cities.itertuples()
        ]

        results = await asyncio.gather(*tasks)

        for row, data in zip(cities.itertuples(), results):
            if data:
                data["city_name"] = row.city_name
                all_data.append(data)
            else:
                log(f"❌ Failed: {row.city_name}")

    return all_data

def extract_aq(**context):
    target_date = context['ds']
    logger.info(f"===== START AQ CRAWL {target_date} =====")
    cities_file = get_last_file_s3('staging/city/')
    if not cities_file:
        logger.error("❌ No city parquet found.")
        return
    
    cities = pd.read_parquet(cities_file)
    logger.info(f"Loaded {len(cities)} cities from {cities_file}")

    start_date = target_date
    end_date = target_date
    logger.info(f"Crawling {target_date}")
    start_time = datetime.now()

    all_data = asyncio.run(crawl_all_cities(cities, start_date, end_date))
    prefix = "raw/aq/"

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_BUCKET_ACESS_KEY,
        aws_secret_access_key=AWS_BUCKET_SECRET_KEY,
        region_name=REGION_NAME
    )

    # Convert JSON to bytes
    data_bytes = json.dumps(all_data, ensure_ascii=False, indent=4).encode("utf-8")

    key = f"{prefix}aq_{target_date}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=data_bytes,
        ContentType="application/json"
    )

    duration = (datetime.now() - start_time).seconds
    logger.info(f"✔ Saved: s3://{BUCKET_NAME}/{key}")
    logger.info(f"⏱ Total time: {duration} seconds")
    logger.info(f"===== FINISHED AQ CRAWL {target_date} =====\n")
        

if __name__ == "__main__":
    setup_logging()
    extract_aq()
