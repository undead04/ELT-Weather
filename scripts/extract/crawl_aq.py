import asyncio
import aiohttp
import pandas as pd
from pathlib import Path
from utils.config import RAW_DIR, PROCESSED_DIR, LOG_DIR
from utils.logging import get_logger
import json
from datetime import datetime,timedelta
from typing import Optional, Dict, Any

LOG_DIR.mkdir(parents=True, exist_ok=True)
logger = get_logger(__name__, domain_file="aq.log")

# ---------- Logging ----------
def log(message: str):
    logger.info(message)

def get_last_file_parquet() -> Path:
    data_dir = PROCESSED_DIR / "city"
    parquet_files = list(data_dir.glob("city_*.parquet"))
    if not parquet_files:
        return None
    return max(parquet_files, key=lambda f: f.stat().st_mtime)

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
                data["city_id"] = row.city_id
                all_data.append(data)
            else:
                log(f"❌ Failed: {row.city_name}")

    return all_data

def extract_aq():
    now_date = datetime.now()
    last_date = now_date - timedelta(1)
    logger.info(f"===== START AQ CRAWL {last_date} =====")
    cities_path = get_last_file_parquet()
    if not cities_path:
        logger.error("❌ No city parquet found.")
        return
    
    cities = pd.read_parquet(cities_path)
    logger.info(f"Loaded {len(cities)} cities from {cities_path.name}")

    start_date = last_date.strftime("%Y-%m-%d")
    end_date = last_date.strftime("%Y-%m-%d")
    logger.info(f"Crawling {last_date}")
    start_time = datetime.now()

    all_data = asyncio.run(crawl_all_cities(cities, start_date, end_date))
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_path = RAW_DIR / "aq" / f"aq_{date_str}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)
    
    duration = (datetime.now() - start_time).seconds
    logger.info(f"✔ Saved: {output_path}")
    logger.info(f"⏱ Total time: {duration} seconds")
    logger.info(f"===== FINISHED AQ CRAWL {last_date} =====\n")
        

if __name__ == "__main__":
    from utils.logging import setup_logging

    setup_logging()
    extract_aq()
