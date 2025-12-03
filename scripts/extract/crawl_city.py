import asyncio
import aiohttp
import json
from pathlib import Path
from utils.config import RAW_DIR
from datetime import datetime
from utils.logging import get_logger

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
    
    # ---- 4. Lưu file ----
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_path = RAW_DIR / "city" / f"city_{date_str}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=4)
    logger = get_logger(__name__, domain_file="city.log")
    logger.info("Saved to: %s", output_path)

def crawl_city():
    asyncio.run(crawl_city_async())
    
if __name__ == "__main__":
    from utils.logging import setup_logging

    setup_logging()
    crawl_city()
