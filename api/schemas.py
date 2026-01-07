from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date

# Shared properties
class CityBase(BaseModel):
    city_name: str
    country: str
    lat: float
    lon: float
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float

class CityResponse(CityBase):
    city_id: int
    inseget_time: datetime
    class Config:
        orm_mode = True

class WeatherResponse(BaseModel):
    city_name: str
    full_date: date
    hour: int
    temperature: float
    humidity: float
    wind_speed: float
    weather_type: str
    cloud_cover: float
    rain: float
    apparent_temperature: float
    wind_direction: float
    precipitation: float
    inseget_time: datetime
    
    class Config:
        orm_mode = True

class AirQualityResponse(BaseModel):
    city_name: str
    full_date: date
    hour: int
    aqi: float
    pm25: float
    pm10: float
    co2: float
    o3: float
    so2: float
    no2: float
    co: float
    inseget_time: datetime

    class Config:
        orm_mode = True
