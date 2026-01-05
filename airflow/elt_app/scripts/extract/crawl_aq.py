import asyncio
import aiohttp
import pandas as pd
from elt_app.utils.config import  BUCKET_NAME, REGION_NAME,AWS_BUCKET_ACESS_KEY,AWS_BUCKET_SECRET_KEY
from elt_app.utils.logging import get_logger, setup_logging
from datetime import datetime,timedelta
from typing import Optional, Dict, Any
from elt_app.utils.utils import get_last_file_s3
import s3fs
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

def extract_aq(**context):
    target_date = context['ds']
    logger.info(f"===== START AQ CRAWL {target_date} =====")
    cities_file = get_last_file_s3('silver/dim_city/', '.parquet')
    if not cities_file: 
        logger.error("❌ No city parquet found.")
        raise ValueError("No city parquet found")
    
    cities = pd.read_parquet(cities_file)
    logger.info(f"Loaded {len(cities)} cities from {cities_file}")

    start_date = target_date
    end_date = target_date
    logger.info(f"Crawling {target_date}")
    start_time = datetime.now()

    all_data = asyncio.run(crawl_all_cities(cities, start_date, end_date))
    
    df = pd.DataFrame(all_data)
    if df.empty:
        logger.info("No data to save")
        raise ValueError("No data to save")
    
    bucket = BUCKET_NAME
    prefix = "bronze/fact_aq/"
    fs = s3fs.S3FileSystem(
        key=AWS_BUCKET_ACESS_KEY,
        secret=AWS_BUCKET_SECRET_KEY,
        client_kwargs={'region_name': REGION_NAME}
    )

    s3_path = f"s3://{bucket}/{prefix}event_date={target_date}/fact_aq.json"
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
    logger.info(f"===== FINISHED AQ CRAWL {target_date} =====\n")
        

if __name__ == "__main__":
    setup_logging()
    extract_aq()
