import asyncio
import aiohttp
import json
from elt_app.utils.config import AWS_BUCKET_ACESS_KEY,AWS_BUCKET_SECRET_KEY,REGION_NAME,BUCKET_NAME
from datetime import datetime
from elt_app.utils.logging import get_logger
import boto3

logger = get_logger(__name__, domain_file="city.log")

# Giới hạn số request đồng thời (Nominatim recommend <= 3)
SEM = asyncio.Semaphore(3)

async def fetch_detail(session:aiohttp.ClientSession, city_name:str):
    """Gọi API Nominatim """
    url = (
        "https://nominatim.openstreetmap.org/search?"
        f"q={city_name}&format=jsonv2&limit=1"
    )

    headers = {
        "User-Agent": "WeatherETL/1.0 (antran.261004@gmail.com)"
    }

    async with SEM:   # tránh gửi quá nhiều req cùng lúc
            async with session.get(url, headers=headers) as resp:
                logger.info('%s → %s', city_name, resp.status)
                if resp.status == 200:
                    data = await resp.json()
                    await asyncio.sleep(1)  # delay bắt buộc
                    return data
                else:
                    return None


async def crawl_city_async():
    # ---- 1. Lấy danh sách tỉnh ----
    url_province = "https://provinces.open-api.vn/api/v1/p/"
    async with aiohttp.ClientSession() as session:
        async with session.get(url_province) as resp:
            if resp.status != 200:
                logger.error("Failed to load city list: HTTP %s", resp.status)
                return
            cities = await resp.json()

    # ---- 2. Tạo task để crawl song song ----
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_detail(session, city["name"])
            for city in cities
        ]

        results = await asyncio.gather(*tasks)

    # ---- 3. Ghép data lại ----
    output_data = []
    for city, detail in zip(cities, results):
        if detail and len(detail) > 0:
            output_data.append({
                "city_id": city["code"],
                "name": city["name"],
                **detail[0]
            })
    

    date_str = datetime.now().strftime("%Y-%m-%d")

    #---- 4. Upload JSON lên S3 ----

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_BUCKET_ACESS_KEY,
        aws_secret_access_key=AWS_BUCKET_SECRET_KEY,
        region_name=REGION_NAME
    )

    bucket = BUCKET_NAME
    prefix = "raw/city/"
    date_str = datetime.now().strftime("%Y-%m-%d")

    # Convert JSON to bytes
    data_bytes = json.dumps(output_data, ensure_ascii=False, indent=4).encode("utf-8")

    key = f"{prefix}city_{date_str}.json"

    # Upload
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data_bytes,
        ContentType="application/json"
    )
    
    logger.info("Uploaded to S3: s3://%s/%s", bucket, key)

def crawl_city():
    asyncio.run(crawl_city_async())
    
if __name__ == "__main__":
    from utils.logging import setup_logging

    setup_logging()
    crawl_city()
