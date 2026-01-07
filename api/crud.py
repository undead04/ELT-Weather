from sqlalchemy.orm import Session
from sqlalchemy import desc
import models, schemas
from datetime import date

def get_city_by_id(db: Session, city_id: int):
    return db.query(models.DimCity).filter(models.DimCity.city_id == city_id).first()
def get_all_city(db: Session):
    return db.query(models.DimCity).all()
def get_list_weather(db: Session, city_id: int,from_date: date, to_date: date):
    return (
        db.query(
            models.DimCity.city_name,
            models.DimDate.full_date,
            models.DimTime.hour,
            models.FactWeather.cloud_cover,
            models.FactWeather.rain,
            models.FactWeather.apparent_temperature,
            models.FactWeather.wind_direction,
            models.FactWeather.precipitation,
            models.FactWeather.temperature,
            models.FactWeather.humidity,
            models.FactWeather.wind_speed,
            models.FactWeather.weather_type,
            models.FactWeather.inseget_time
        )
        .join(models.DimTime, models.FactWeather.time_id == models.DimTime.time_id)
        .join(models.DimCity, models.FactWeather.city_id == models.DimCity.city_id)
        .join(models.DimDate, models.FactWeather.date_id == models.DimDate.date_id)
        .filter(models.DimCity.city_id == city_id)
        .filter(models.DimDate.full_date >= from_date)
        .filter(models.DimDate.full_date <= to_date)
        .order_by(desc(models.DimDate.full_date))
        .all()
    )

def get_list_air_quality(db: Session, city_id: int,from_date: date, to_date: date):
    return (
        db.query(
            models.DimCity.city_name,
            models.DimDate.full_date,
            models.DimTime.hour,
            models.FactAirQuality.aqi,
            models.FactAirQuality.pm25,
            models.FactAirQuality.pm10,
            models.FactAirQuality.o3,
            models.FactAirQuality.so2,
            models.FactAirQuality.no2,
            models.FactAirQuality.co,
            models.FactAirQuality.co2,
            models.FactAirQuality.inseget_time
        )
        .join(models.DimTime,models.FactAirQuality.time_id == models.DimTime.time_id)
        .join(models.DimCity, models.FactAirQuality.city_id == models.DimCity.city_id)
        .join(models.DimDate, models.FactAirQuality.date_id == models.DimDate.date_id)
        .filter(models.DimCity.city_id == city_id)
        .filter(models.DimDate.full_date >= from_date)
        .filter(models.DimDate.full_date <= to_date)
        .order_by(desc(models.DimDate.full_date))
        .all()
    )
