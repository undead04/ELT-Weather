from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Date, UniqueConstraint
from sqlalchemy.orm import relationship
from database import Base

class DimCity(Base):
    __tablename__ = "dim_city"

    city_id = Column(Integer, primary_key=True, index=True)
    city_name = Column(String, unique=True, index=True, nullable=False)
    country = Column(String, nullable=False)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    min_lat = Column(Float, nullable=False)
    max_lat = Column(Float, nullable=False)
    min_lon = Column(Float, nullable=False)
    max_lon = Column(Float, nullable=False)
    inseget_time = Column(DateTime, nullable=False)

class DimDate(Base):
    __tablename__ = "dim_date"

    date_id = Column(Integer, primary_key=True, index=True)
    full_date = Column(Date, unique=True, nullable=False)
    day = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    is_weekend = Column(Boolean, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    inseget_time = Column(DateTime, nullable=False)
    __table_args__ = (UniqueConstraint('day', 'month', 'year', name='uq_dim_date_dmy'),)

class DimTime(Base):
    __tablename__ = "dim_time"

    time_id = Column(Integer, primary_key=True, index=True)
    hour = Column(Integer, nullable=False)
    minute = Column(Integer, nullable=False)
    second = Column(Integer, nullable=False)
    time_bucket = Column(String, nullable=False)
    inseget_time = Column(DateTime, nullable=False)
    __table_args__ = (UniqueConstraint('hour', 'minute', 'second', name='uq_dim_time_hms'),)

class FactWeather(Base):
    __tablename__ = "fact_weather"

    weather_id = Column(Integer, primary_key=True, index=True)
    date_id = Column(Integer, ForeignKey("dim_date.date_id"), nullable=False)
    city_id = Column(Integer, ForeignKey("dim_city.city_id"), nullable=False)
    time_id = Column(Integer, ForeignKey("dim_time.time_id"), nullable=False)
    inseget_time = Column(DateTime, nullable=False)

    temperature = Column(Float, nullable=False)
    humidity = Column(Float, nullable=False)
    wind_speed = Column(Float, nullable=False)
    precipitation = Column(Float, nullable=False)
    weather_type = Column(String, nullable=False)
    weather_code = Column(Integer, nullable=False)
    cloud_cover = Column(Float, nullable=False)
    rain = Column(Float, nullable=False)
    wind_direction = Column(String, nullable=False)
    apparent_temperature = Column(Float, nullable=False)

    city = relationship("DimCity")
    date = relationship("DimDate")
    time = relationship("DimTime")

class FactAirQuality(Base):
    __tablename__ = "fact_air_quality"

    aq_id = Column(Integer, primary_key=True, index=True)
    date_id = Column(Integer, ForeignKey("dim_date.date_id"), nullable=False)
    city_id = Column(Integer, ForeignKey("dim_city.city_id"), nullable=False)
    time_id = Column(Integer, ForeignKey("dim_time.time_id"), nullable=False)
    inseget_time = Column(DateTime, nullable=False)

    aqi = Column(Float, nullable=False)
    pm25 = Column(Float, nullable=False)
    pm10 = Column(Float, nullable=False)
    no2 = Column(Float, nullable=False)
    so2 = Column(Float, nullable=False)
    o3 = Column(Float, nullable=False)
    co = Column(Float, nullable=False)
    co2 = Column(Float, nullable=False)

    city = relationship("DimCity")
    date = relationship("DimDate")
    time = relationship("DimTime")
